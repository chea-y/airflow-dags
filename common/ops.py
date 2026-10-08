"""운영 공통 설정: 재시도 기본값 + 실패 알림 메일"""
import logging
import smtplib
from datetime import timedelta
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from urllib.parse import quote

import pendulum
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)
SMTP_CONN_ID = "smtp_report"                 # D-3에서 만든 네이버 메일 Connection
AIRFLOW_URL = "http://localhost:8080"        # 메일 속 바로가기 링크용

# ── 모든 DAG 공통 재시도 정책 ───────────────────────────
DEFAULT_ARGS = {
    "retries": 3,                              # 최대 3번 재시도
    "retry_delay": timedelta(minutes=1),       # 첫 재시도는 1분 뒤
    "retry_exponential_backoff": True,         # 1분 → 2분 → 4분 … 점점 길게 (지수 백오프)
    "max_retry_delay": timedelta(minutes=10),  # 아무리 길어도 10분까지
    "execution_timeout": timedelta(minutes=10),  # 태스크 하나가 10분 넘으면 강제 종료
}


def send_mail(subject, body, recipients=None):
    """smtp_report Connection으로 메일 발송"""
    conn = BaseHook.get_connection(SMTP_CONN_ID)
    sender = conn.extra_dejson.get("from_email", conn.login)
    if recipients is None:
        # 운영 알림 받는 사람: alert_recipients가 없으면 리포트 받는 사람에게
        raw = Variable.get("alert_recipients", default_var=None) or Variable.get("er_report_recipients")
        recipients = [x.strip() for x in raw.split(",") if x.strip()]

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = formataddr(("Airflow 운영 알림", sender))
    msg["To"] = ", ".join(recipients)

    port = conn.port or 465
    if port == 465:
        server = smtplib.SMTP_SSL(conn.host, port, timeout=30)
    else:
        server = smtplib.SMTP(conn.host, port, timeout=30)
    with server:
        if port != 465:
            server.starttls()
        server.login(conn.login, conn.password)
        server.send_message(msg, from_addr=sender, to_addrs=recipients)


def notify_dag_failure(context):
    """DAG 실행이 최종 실패했을 때 한 번만 호출됨 (재시도 중에는 호출 안 됨)"""
    try:
        dag_run = context["dag_run"]
        failed = dag_run.get_task_instances(state=[TaskInstanceState.FAILED])
        lines = []
        for ti in failed:
            name = ti.task_id + (f"[{ti.map_index}]" if ti.map_index >= 0 else "")
            lines.append(f"- {name} (시도 {ti.try_number}회)")

        now = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d %H:%M")
        url = f"{AIRFLOW_URL}/dags/{dag_run.dag_id}/grid?dag_run_id={quote(dag_run.run_id)}"
        body = "\n".join([
            f"DAG: {dag_run.dag_id}",
            f"실행 ID: {dag_run.run_id}",
            f"알림 시각: {now} (KST)",
            f"사유: {context.get('reason', '-')}",
            "",
            "실패한 태스크:",
            *(lines or ["- (확인 필요)"]),
            "",
            f"바로가기: {url}",
            "",
            "※ 재시도를 모두 실패한 경우에만 발송됩니다.",
        ])
        send_mail(f"[Airflow 실패] {dag_run.dag_id} ({now})", body)
        log.info("실패 알림 메일 발송 완료: %s", dag_run.dag_id)
    except Exception:
        # 알림이 실패해도 DAG 처리에는 영향 없도록 로그만 남김
        log.exception("실패 알림 메일 발송 중 오류")
