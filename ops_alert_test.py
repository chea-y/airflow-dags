import pendulum
from airflow.decorators import dag, task
from common.ops import DEFAULT_ARGS, notify_dag_failure


@dag(
    dag_id="ops_alert_test",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},   # 테스트라 재시도 없이 바로 실패
    on_failure_callback=notify_dag_failure,
    tags=["ops", "test"],
)
def ops_alert_test():

    @task
    def boom():
        raise ValueError("실패 알림 테스트용으로 일부러 낸 오류입니다")

    boom()


ops_alert_test()