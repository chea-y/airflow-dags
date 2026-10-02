# ── 필요한 도구 불러오기 ──────────────────────────────
from datetime import datetime              # 날짜/시간 도구
import csv                                 # CSV 파일 쓰기 도구
import os                                  # 폴더 만들기 도구

import requests                            # API 요청 도구
from airflow.decorators import dag, task   # 함수를 DAG/태스크로 만드는 도구
from airflow.providers.postgres.hooks.postgres import PostgresHook  # PostgreSQL 접속 도구

DATA_DIR = "/opt/airflow/data"             # CSV 저장 폴더 (= 내 PC의 ~/data)
CONN_ID = "my_postgres"                    # Airflow에 등록한 DB 접속 정보 이름
SCHEMA = "airfy"                           # 저장할 스키마 (소문자라 따옴표 불필요)
TABLE = f"{SCHEMA}.weather_hourly"         # 최종 테이블 이름: airfy.weather_hourly


# ── DAG 정의 ──────────────────────────────────────────
@dag(
    dag_id="weather_etl",                  # DAG 이름
    start_date=datetime(2026, 9, 1),       # 스케줄 시작 기준일
    schedule="@daily",                     # 하루 한 번 실행
    catchup=False,                         # 지난 날짜 몰아서 실행 안 함
    tags=["tutorial", "etl"],              # 검색용 꼬리표
)
def weather_etl():

    # ── E: 추출 ──────────────────────────────────────
    @task
    def extract(ds=None):                  # ds = 처리할 날짜 (예: 2026-09-28)
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 37.5665,           # 서울 위도
            "longitude": 126.9780,         # 서울 경도
            "hourly": "temperature_2m,relative_humidity_2m",  # 시간별 기온, 습도
            "timezone": "Asia/Seoul",      # 한국 시간 기준
            "start_date": ds,              # 하루치만
            "end_date": ds,
        }
        res = requests.get(url, params=params, timeout=30)  # API 호출
        res.raise_for_status()             # 실패하면 에러 → 태스크 실패
        hourly = res.json()["hourly"]      # 시간별 데이터만 꺼내기
        print(f"{len(hourly['time'])}건 수집")
        return hourly                      # 다음 태스크로 전달

    # ── T: 변환 ──────────────────────────────────────
    @task
    def transform(hourly):
        rows = []
        for t, temp, hum in zip(           # 시간/기온/습도를 한 줄씩 묶기
            hourly["time"],
            hourly["temperature_2m"],
            hourly["relative_humidity_2m"],
        ):
            if temp is None:               # 빈 값은 건너뛰기
                continue
            if temp >= 28:                 # 온도 구분 만들기
                level = "더움"
            elif temp <= 5:
                level = "추움"
            else:
                level = "보통"
            rows.append({
                "time": t.replace("T", " "),  # "2026-09-28T13:00" → "2026-09-28 13:00"
                "temperature": temp,
                "humidity": hum,
                "level": level,
            })

        temps = [r["temperature"] for r in rows]
        print(f"평균 {sum(temps)/len(temps):.1f}℃ / 최고 {max(temps)}℃ / 최저 {min(temps)}℃")
        return rows

    # ── L-1: CSV 파일로 저장 ─────────────────────────
    @task
    def load(rows, ds=None):
        os.makedirs(DATA_DIR, exist_ok=True)        # 폴더 없으면 만들기
        path = f"{DATA_DIR}/weather_{ds}.csv"       # 날짜별 파일 이름
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["time", "temperature", "humidity", "level"])
            writer.writeheader()                    # 첫 줄: 컬럼 이름
            writer.writerows(rows)                  # 나머지: 데이터
        print(f"{len(rows)}건 CSV 저장 완료 → {path}")

    # ── L-2: 회사 PostgreSQL에 저장 ──────────────────
    @task
    def load_db(rows):
        hook = PostgresHook(postgres_conn_id=CONN_ID)   # 등록한 접속 정보로 DB 연결
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                ts          TIMESTAMP PRIMARY KEY,  -- 시간 (중복 불가)
                temperature REAL,                   -- 기온
                humidity    REAL,                   -- 습도
                level       VARCHAR(10)             -- 온도 구분
            )
        """)                                        # 테이블 없으면 만들기
        hook.insert_rows(
            table=TABLE,
            rows=[(r["time"], r["temperature"], r["humidity"], r["level"]) for r in rows],
            target_fields=["ts", "temperature", "humidity", "level"],
            replace=True,                           # 같은 시간 데이터는 덮어쓰기
            replace_index="ts",                     # 같은지 판단하는 기준 컬럼
        )
        print(f"{len(rows)}건 DB 저장 완료 → {TABLE}")

    # ── 실행 순서 ────────────────────────────────────
    rows = transform(extract())            # 추출 → 변환
    load(rows)                             # CSV 저장   ┐ 동시에
    load_db(rows)                          # DB 저장    ┘ 실행


weather_etl()                              # DAG 등록