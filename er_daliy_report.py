# ── 필요한 도구 불러오기 ──────────────────────────────
from datetime import timedelta
import json
import smtplib                             # 메일 발송 도구 (파이썬 기본)
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr

import pendulum                            # 시간대(한국 시간) 처리 도구
import requests                            # API 요청 도구
from airflow.datasets import Dataset       # DAG 간 연결 신호
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook    # Connection 정보 꺼내는 도구
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

KST = "Asia/Seoul"
CONN_ID = "my_postgres"                    # 회사 DB
DIFY_CONN_ID = "dify_report_app"           # Dify 워크플로 앱
SMTP_CONN_ID = "smtp_report"               # 메일 서버
RECIPIENTS_VAR = "er_report_recipients"    # 받는 사람 목록 (쉼표 구분)

SCHEMA = "airfy"
SNAPSHOT = f"{SCHEMA}.er_beds_snapshot"
REGION_HOURLY = f"{SCHEMA}.v_er_beds_region_hourly"
REPORT_TABLE = f"{SCHEMA}.er_report_daily"
ER_REPORT_DATASET = Dataset("airfy://er_report_daily")   # "리포트 갱신됨" 신호 이름

# 집계 기간 조건: (시작, 끝] → 09시~다음날 08시 = 24회
WINDOW = "snapshot_at > %(start)s AND snapshot_at <= %(end)s"


def resolve_window(data_interval_end, params):
    """집계 기간 계산. 테스트 시 params.window_end로 기준 시각을 바꿀 수 있음"""
    if params.get("window_end"):
        end = pendulum.parse(params["window_end"], tz=KST)
    else:
        end = data_interval_end.in_timezone(KST)   # 정기 실행: 오늘 08:00 KST
    start = end.subtract(hours=24)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt), end.strftime("%Y-%m-%d")


# ── DAG 정의 ──────────────────────────────────────────
@dag(
    dag_id="er_daily_report",
    start_date=pendulum.datetime(2026, 10, 1, tz=KST),  # 시간대를 KST로 → cron도 KST 기준
    schedule="0 8 * * *",                  # 매일 08:00 (한국 시간)
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    params={
        "window_end": None,                # 테스트용: "2026-10-03 08:00:00" 형식
        "min_snapshots": 12,               # 24회 중 최소 12회 이상 수집돼야 발송
    },
    tags=["medical", "dify", "report"],
)
def er_daily_report():

    # ── 1. 집계: 지난 24시간 숫자를 SQL로 계산 ─────────
    @task
    def build_summary(params=None, data_interval_end=None):
        start, end, report_date = resolve_window(data_interval_end, params)
        p = {"start": start, "end": end}
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        print(f"집계 기간: {start} 초과 ~ {end} 이하 (보고일 {report_date})")

        first, last, snapshots = hook.get_first(f"""
            SELECT MIN(snapshot_at), MAX(snapshot_at), COUNT(DISTINCT snapshot_at)
            FROM {SNAPSHOT} WHERE {WINDOW}
        """, parameters=p)

        regions = hook.get_records(f"""
            SELECT region,
                   ROUND(AVG(available_rate), 1),
                   MIN(available_beds),
                   MAX(full_cnt),
                   ROUND(AVG(full_cnt), 1)
            FROM {REGION_HOURLY} WHERE {WINDOW}
            GROUP BY region
            ORDER BY 2
        """, parameters=p)

        peaks = {}
        for region, peak_at, peak_rate in hook.get_records(f"""
            SELECT DISTINCT ON (region) region, snapshot_at, available_rate
            FROM {REGION_HOURLY} WHERE {WINDOW}
            ORDER BY region, available_rate ASC NULLS LAST
        """, parameters=p):
            peaks[region] = (peak_at, peak_rate)

        top_full = hook.get_records(f"""
            SELECT region, name,
                   COUNT(*) FILTER (WHERE status = '포화'),
                   COUNT(*)
            FROM {SNAPSHOT} WHERE {WINDOW}
            GROUP BY hpid, region, name
            HAVING COUNT(*) FILTER (WHERE status = '포화') > 0
            ORDER BY 3 DESC, name
            LIMIT 10
        """, parameters=p)

        summary = {
            "period": {"start": first, "end": last, "snapshots": snapshots},
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
        summary_json = json.dumps(summary, ensure_ascii=False, default=str)
        print(f"집계 완료: 수집 {snapshots}회, {len(summary_json)}자")

        # XCom에는 문자열·숫자만 담기 (날짜·Decimal 오류 방지)
        return {
            "report_date": report_date,
            "period_start": str(first) if first else None,
            "period_end": str(last) if last else None,
            "snapshots": snapshots,
            "summary_json": summary_json,
        }

    # ── 2. 데이터 충분한지 확인: 부족하면 아래 태스크 전부 건너뜀 ──
    @task.short_circuit
    def has_enough_data(meta, params=None):
        minimum = int(params.get("min_snapshots", 12))
        ok = meta["snapshots"] >= minimum
        print(f"수집 {meta['snapshots']}회 / 기준 {minimum}회 → {'발송 진행' if ok else '발송 건너뜀'}")
        return ok

    # ── 3. Dify 호출: LLM이 리포트 작성 ───────────────
    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )
    def call_dify(meta):
        conn = BaseHook.get_connection(DIFY_CONN_ID)
        base_url = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
        res = requests.post(
            f"{base_url.rstrip('/')}/workflows/run",
            headers={"Authorization": f"Bearer {conn.password}",
                     "Content-Type": "application/json"},
            json={
                "inputs": {"report_date": meta["report_date"],
                           "summary_json": meta["summary_json"]},
                "response_mode": "blocking",
                "user": "airflow-er-daily-report",
            },
            timeout=120,
        )
        if res.status_code != 200:
            raise ValueError(f"Dify 호출 실패 {res.status_code}: {res.text[:500]}")

        body = res.json()
        data = body["data"]
        if data["status"] != "succeeded":
            raise ValueError(f"Dify 워크플로 실패: {data.get('error')}")

        print(f"Dify 완료: {data.get('elapsed_time')}초 / 토큰 {data.get('total_tokens')}")
        return {
            "report": data["outputs"]["report"],
            "run_id": body.get("workflow_run_id"),
            "tokens": data.get("total_tokens"),
        }

    # ── 4. 리포트 DB 저장 (같은 날짜는 덮어쓰기) ──────────
    #   outlets: 이 태스크가 성공하면 "리포트 갱신됨" 신호 → er_report_kb_sync 자동 실행
    @task(outlets=[ER_REPORT_DATASET])
    def save_report(meta, result):
        fields = ["report_date", "period_start", "period_end", "snapshots",
                  "summary_json", "report_text", "dify_run_id", "total_tokens",
                  "kb_synced_at"]                                # NULL로 초기화 → 지식베이스 재동기화 대상
        row = (meta["report_date"], meta["period_start"], meta["period_end"],
               meta["snapshots"], meta["summary_json"],
               result["report"], result["run_id"], result["tokens"],
               None)
        PostgresHook(postgres_conn_id=CONN_ID).insert_rows(
            table=REPORT_TABLE,
            rows=[row],
            target_fields=fields,
            replace=True,
            replace_index=["report_date"],
        )
        print(f"리포트 저장 → {REPORT_TABLE} ({meta['report_date']})")

    # ── 5. 메일 발송 + 발송 시각 기록 ──────────────────
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def send_email(meta, result):
        conn = BaseHook.get_connection(SMTP_CONN_ID)
        sender = conn.extra_dejson.get("from_email", conn.login)
        recipients = [x.strip() for x in Variable.get(RECIPIENTS_VAR).split(",") if x.strip()]

        body = (
            result["report"]
            + "\n\n────────────────────\n"
            + "이 메일은 Airflow(er_daily_report)가 자동 발송했습니다.\n"
            + "원천: 국립중앙의료원 응급실 실시간 가용병상 정보(공공데이터포털)\n"
            + "리포트 문장은 AI가 작성했으며, 숫자는 수집 데이터 집계 결과입니다."
        )
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(f"[응급실 브리핑] {meta['report_date']}", "utf-8")
        msg["From"] = formataddr(("응급실 리포트 봇", sender))
        msg["To"] = ", ".join(recipients)

        port = conn.port or 465
        if port == 465:                                  # SSL 방식 (Gmail 기본)
            server = smtplib.SMTP_SSL(conn.host, port, timeout=30)
        else:                                            # 587: STARTTLS 방식
            server = smtplib.SMTP(conn.host, port, timeout=30)
            server.starttls()
        with server:
            server.login(conn.login, conn.password)
            server.send_message(msg, from_addr=sender, to_addrs=recipients)
        print(f"메일 발송 완료 → {', '.join(recipients)}")

        PostgresHook(postgres_conn_id=CONN_ID).run(
            f"UPDATE {REPORT_TABLE} SET sent_at = now() WHERE report_date = %s",
            parameters=(meta["report_date"],),
        )

    # ── 실행 순서 ────────────────────────────────────
    meta = build_summary()
    gate = has_enough_data(meta)
    result = call_dify(meta)
    gate >> result                             # 데이터 부족하면 Dify 호출부터 건너뜀
    save_report(meta, result) >> send_email(meta, result)


er_daily_report()