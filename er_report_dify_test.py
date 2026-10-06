# ── 필요한 도구 불러오기 ──────────────────────────────
from datetime import datetime, timedelta
import json

import pendulum                            # 시간대(한국 시간) 처리 도구
import requests                            # API 요청 도구
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook    # Connection 정보 꺼내는 도구
from airflow.providers.postgres.hooks.postgres import PostgresHook  # DB 접속 도구

CONN_ID = "my_postgres"                    # 회사 DB
DIFY_CONN_ID = "dify_report_app"           # Dify 워크플로 앱 (Host=API 주소, Password=API 키)
SCHEMA = "airfy"
SNAPSHOT = f"{SCHEMA}.er_beds_snapshot"
REGION_HOURLY = f"{SCHEMA}.v_er_beds_region_hourly"

# 집계 기간: DB에 있는 최신 시각 기준 최근 24시간 (테스트용)
WINDOW = f"""
    snapshot_at > (SELECT MAX(snapshot_at) FROM {SNAPSHOT}) - INTERVAL '24 hours'
"""


# ── DAG 정의 ──────────────────────────────────────────
@dag(
    dag_id="er_report_dify_test",
    start_date=datetime(2026, 10, 1),
    schedule=None,                         # 수동 실행 전용 (테스트)
    catchup=False,
    tags=["medical", "dify", "test"],
)
def er_report_dify_test():

    # ── 1. 집계: LLM에 보낼 숫자를 SQL로 미리 계산 ──────
    @task
    def build_summary():
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # 기간 정보
        start, end, snapshots = hook.get_first(f"""
            SELECT MIN(snapshot_at), MAX(snapshot_at), COUNT(DISTINCT snapshot_at)
            FROM {SNAPSHOT} WHERE {WINDOW}
        """)
        if not snapshots:
            raise ValueError("집계할 데이터가 없어요. er_beds_collect가 켜져 있는지 확인하세요.")

        # 지역별 요약 (평균 가용률 낮은 순)
        regions = hook.get_records(f"""
            SELECT region,
                   ROUND(AVG(available_rate), 1) AS avg_rate,   -- 평균 가용률(%)
                   MIN(available_beds)           AS min_beds,   -- 가장 적었던 남은 병상 합계
                   MAX(full_cnt)                 AS max_full,   -- 포화 기관 수 최대
                   ROUND(AVG(full_cnt), 1)       AS avg_full    -- 포화 기관 수 평균
            FROM {REGION_HOURLY} WHERE {WINDOW}
            GROUP BY region
            ORDER BY avg_rate
        """)

        # 지역별 가장 붐볐던 시각 (가용률 최저)
        peaks = dict()
        for region, peak_at, peak_rate in hook.get_records(f"""
            SELECT DISTINCT ON (region) region, snapshot_at, available_rate
            FROM {REGION_HOURLY} WHERE {WINDOW}
            ORDER BY region, available_rate ASC NULLS LAST
        """):
            peaks[region] = (peak_at, peak_rate)

        # 포화 시간이 길었던 병원 Top 10
        top_full = hook.get_records(f"""
            SELECT region, name,
                   COUNT(*) FILTER (WHERE status = '포화') AS full_hours,
                   COUNT(*)                                AS total_hours
            FROM {SNAPSHOT} WHERE {WINDOW}
            GROUP BY hpid, region, name
            HAVING COUNT(*) FILTER (WHERE status = '포화') > 0
            ORDER BY full_hours DESC, name
            LIMIT 10
        """)

        summary = {
            "period": {"start": start, "end": end, "snapshots": snapshots},
            "regions": [
                {
                    "region": r, "avg_rate": rate, "min_beds": beds,
                    "max_full": mx, "avg_full": av,
                    "peak_at": peaks.get(r, (None, None))[0],
                    "peak_rate": peaks.get(r, (None, None))[1],
                }
                for r, rate, beds, mx, av in regions
            ],
            "top_full_hospitals": [
                {"region": r, "name": n, "full_hours": f, "total_hours": t}
                for r, n, f, t in top_full
            ],
        }
        # default=str: 날짜·소수(Decimal)를 문자열로 바꿔서 JSON 오류 방지
        summary_json = json.dumps(summary, ensure_ascii=False, default=str)
        print(f"집계 완료 ({len(summary_json)}자)\n{summary_json}")
        return summary_json

    # ── 2. Dify 워크플로 호출: LLM이 리포트 문장 작성 ────
    @task(
        retries=3,                                    # LLM은 가끔 느리거나 실패 → 재시도
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),       # 3분 넘으면 강제 종료
    )
    def call_dify(summary_json):
        conn = BaseHook.get_connection(DIFY_CONN_ID)
        base_url = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
        report_date = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")

        res = requests.post(
            f"{base_url.rstrip('/')}/workflows/run",
            headers={
                "Authorization": f"Bearer {conn.password}",   # API 키
                "Content-Type": "application/json",
            },
            json={
                "inputs": {                                    # 시작 노드 변수 이름과 같아야 함
                    "report_date": report_date,
                    "summary_json": summary_json,
                },
                "response_mode": "blocking",                   # 결과 나올 때까지 기다림
                "user": "airflow-er-report",                   # Dify 로그에 표시될 사용자
            },
            timeout=120,
        )
        if res.status_code != 200:
            raise ValueError(f"Dify 호출 실패 {res.status_code}: {res.text[:500]}")

        data = res.json()["data"]
        if data["status"] != "succeeded":
            raise ValueError(f"Dify 워크플로 실패: {data.get('error')}")

        report = data["outputs"]["report"]                     # 종료 노드 변수 이름
        print(f"소요 {data.get('elapsed_time')}초 / 토큰 {data.get('total_tokens')}")
        print("=" * 50)
        print(report)
        return report

    # ── 실행 순서 ────────────────────────────────────
    call_dify(build_summary())


er_report_dify_test()