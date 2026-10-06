# ── 필요한 도구 불러오기 ──────────────────────────────
from datetime import date, timedelta
import json
import time

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException   # 태스크를 '실패' 대신 '건너뜀'으로 끝내는 도구
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

KST = "Asia/Seoul"
CONN_ID = "my_postgres"
KB_CONN_ID = "dify_kb"                         # Host=API 주소, Password=지식베이스 키, Extra.dataset_id
REPORT_TABLE = "airfy.er_report_daily"
ER_REPORT_DATASET = Dataset("airfy://er_report_daily")   # er_daily_report와 같은 이름이어야 연결됨
WEEKDAYS = "월화수목금토일"


def kb_request(method, path, **kwargs):
    """Dify 지식베이스 API 호출 (dataset_id까지 붙인 주소로)"""
    conn = BaseHook.get_connection(KB_CONN_ID)
    base = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
    dataset_id = conn.extra_dejson["dataset_id"]
    url = f"{base.rstrip('/')}/datasets/{dataset_id}{path}"
    return requests.request(
        method, url,
        headers={"Authorization": f"Bearer {conn.password}"},
        timeout=60, **kwargs,
    )


def short_time(value):
    """'2026-10-06 10:00:00' → '10-06 10:00'"""
    return str(value)[5:16] if value else "-"


def build_document(r):
    """지식베이스에 올릴 문서 본문. 날짜·요일·주간 정보를 넣어서 검색이 잘 되게 함"""
    d = date.fromisoformat(r["report_date"])
    week_start = d - timedelta(days=d.weekday())
    week_end = week_start + timedelta(days=6)
    s = r["summary"] or {}

    lines = [
        f"[응급실 일일 브리핑] {d.year}년 {d.month}월 {d.day}일 ({WEEKDAYS[d.weekday()]})",
        "",
        f"- 보고일: {d.isoformat()}",
        f"- 해당 주간: {week_start.isoformat()}(월) ~ {week_end.isoformat()}(일), {d.year}년 {d.isocalendar()[1]}주차",
        f"- 집계 기간: {r['period_start']} ~ {r['period_end']} (수집 {r['snapshots']}회)",
        "- 출처: 국립중앙의료원 응급실 실시간 가용병상 정보(공공데이터포털)",
        "- 수집 지역: 서울특별시, 경기도, 인천광역시, 부산광역시, 대구광역시",
        "",
        "■ 지역별 집계 (평균 가용률 낮은 순)",
    ]
    for g in s.get("regions", []):
        lines.append(
            f"- {g['region']}: 평균 가용률 {g['avg_rate']}%, "
            f"포화 병원 평균 {g['avg_full']}곳(최대 {g['max_full']}곳), "
            f"가장 붐빈 시각 {short_time(g.get('peak_at'))} (가용률 {g.get('peak_rate')}%)"
        )

    lines += ["", "■ 포화 시간이 길었던 병원"]
    tops = s.get("top_full_hospitals", [])
    if tops:
        for h in tops:
            lines.append(f"- {h['region']} {h['name']}: 포화 {h['full_hours']}시간 / 수집 {h['total_hours']}시간")
    else:
        lines.append("- 해당 없음")

    lines += ["", "■ AI 브리핑 원문", r["report_text"]]
    return "\n".join(lines)


# ── DAG 정의 ──────────────────────────────────────────
@dag(
    dag_id="er_report_kb_sync",
    start_date=pendulum.datetime(2026, 10, 1, tz=KST),
    schedule=[ER_REPORT_DATASET],          # 시간표 대신 "리포트 갱신됨" 신호로 실행
    catchup=False,
    max_active_runs=1,
    params={"limit": 30},                  # 한 번에 올릴 최대 리포트 수
    tags=["medical", "dify", "knowledge"],
)
def er_report_kb_sync():

    # ── 1. 동기화 대상 찾기: 리포트는 있는데 아직 안 올린 날짜 ──
    @task
    def find_targets(params=None):
        rows = PostgresHook(postgres_conn_id=CONN_ID).get_records(f"""
            SELECT report_date, period_start, period_end, snapshots, summary_json, report_text
            FROM {REPORT_TABLE}
            WHERE report_text IS NOT NULL
              AND kb_synced_at IS NULL
            ORDER BY report_date
            LIMIT %(limit)s
        """, parameters={"limit": int(params.get("limit", 30))})

        targets = []
        for rd, ps, pe, snaps, sj, text in rows:
            targets.append({
                "report_date": rd.isoformat(),
                "period_start": str(ps) if ps else "-",
                "period_end": str(pe) if pe else "-",
                "snapshots": snaps,
                # jsonb는 보통 dict로 오지만, 문자열로 오는 경우도 대비
                "summary": sj if isinstance(sj, dict) else json.loads(sj or "{}"),
                "report_text": text,
            })
        print(f"동기화 대상 {len(targets)}건: {[t['report_date'] for t in targets]}")
        return targets                     # 0건이면 아래 태스크는 자동으로 skipped

    # ── 2. 리포트 1건 → Dify 문서 생성 또는 수정 ──────────
    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
        max_active_tis_per_dagrun=1,       # 한 번에 1건씩 (Dify 호출 몰림 방지)
    )
    def sync_report(report):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        report_date = report["report_date"]
        name = f"응급실 브리핑 {report_date}"
        text = build_document(report)

        # 문서 ID는 매번 DB에서 다시 읽음 → 재시도해도 문서가 중복 생성되지 않음
        doc_id = hook.get_first(
            f"SELECT kb_document_id FROM {REPORT_TABLE} WHERE report_date = %s",
            parameters=(report_date,),
        )[0]

        res, action = None, None
        if doc_id:
            action = "수정"
            res = kb_request("POST", f"/documents/{doc_id}/update-by-text",
                             json={"name": name, "text": text})
            if res.status_code == 404:     # Dify에서 문서를 지운 경우 → 새로 생성
                print("Dify에 문서가 없어서 새로 생성합니다")
                doc_id = None
            elif res.status_code == 400 and "not available" in res.text:
                # 문서가 Dify에서 아직 처리 중(대기/인덱싱)이거나 비활성 상태 → 지금은 수정 불가
                # kb_synced_at을 NULL로 두고 끝내면, 다음 실행 때 자동으로 다시 시도함
                raise AirflowSkipException(
                    f"[{report_date}] Dify 문서가 아직 처리 중이라 수정을 보류합니다 "
                    f"(doc_id={doc_id}). 다음 실행 때 다시 시도합니다."
                )
        if not doc_id:
            action = "생성"
            res = kb_request("POST", "/document/create-by-text", json={
                "name": name,
                "text": text,
                "indexing_technique": "high_quality",
                "process_rule": {"mode": "automatic"},
            })

        if res.status_code != 200:
            raise ValueError(f"Dify 지식베이스 {action} 실패 {res.status_code}: {res.text[:500]}")

        body = res.json()
        doc_id = body["document"]["id"]
        batch = body.get("batch")

        # 문서 ID는 즉시 저장 (이후 단계가 실패해도 다음엔 '수정'으로 처리)
        hook.run(f"UPDATE {REPORT_TABLE} SET kb_document_id = %s WHERE report_date = %s",
                 parameters=(doc_id, report_date))

        # 인덱싱 완료 대기 (최대 약 60초)
        status = "확인 못 함"
        for _ in range(12):
            if not batch:
                break
            st = kb_request("GET", f"/documents/{batch}/indexing-status")
            if st.status_code == 200 and st.json().get("data"):
                status = st.json()["data"][0].get("indexing_status")
                if status == "completed":
                    break
                if status == "error":
                    raise ValueError(f"인덱싱 실패: {st.text[:300]}")
            time.sleep(5)

        hook.run(f"UPDATE {REPORT_TABLE} SET kb_synced_at = now() WHERE report_date = %s",
                 parameters=(report_date,))
        print(f"[{report_date}] 문서 {action} 완료 (id={doc_id}, 인덱싱={status}, {len(text)}자)")

    # ── 실행 순서 ────────────────────────────────────
    sync_report.expand(report=find_targets())  # 대상 수만큼 태스크 자동 생성


er_report_kb_sync()