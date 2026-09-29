from datetime import datetime
import csv
import os

import requests
from airflow.decorators import dag, task

DATA_DIR = "/opt/airflow/data"   # 컨테이너 안 경로 (= 내 PC의 ~/data)


@dag(
    dag_id="weather_etl",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",      # 하루에 한 번 실행
    catchup=False,          # 과거 날짜는 몰아서 실행하지 않음
    tags=["tutorial", "etl"],
)
def weather_etl():

    @task
    def extract(ds=None):
        """E: 실행 날짜(ds) 하루치 서울 시간별 날씨 가져오기"""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 37.5665,
            "longitude": 126.9780,
            "hourly": "temperature_2m,relative_humidity_2m",
            "timezone": "Asia/Seoul",
            "start_date": ds,
            "end_date": ds,
        }
        res = requests.get(url, params=params, timeout=30)
        res.raise_for_status()          # 요청 실패 시 에러 → 태스크 실패 처리
        hourly = res.json()["hourly"]
        print(f"{len(hourly['time'])}건 수집")
        return hourly

    @task
    def transform(hourly):
        """T: 컬럼 정리 + 온도 구분(level) 추가"""
        rows = []
        for t, temp, hum in zip(
            hourly["time"],
            hourly["temperature_2m"],
            hourly["relative_humidity_2m"],
        ):
            if temp is None:            # 빈 값은 제외
                continue
            if temp >= 28:
                level = "더움"
            elif temp <= 5:
                level = "추움"
            else:
                level = "보통"
            rows.append({
                "time": t.replace("T", " "),
                "temperature": temp,
                "humidity": hum,
                "level": level,
            })

        temps = [r["temperature"] for r in rows]
        print(f"평균 {sum(temps)/len(temps):.1f}℃ / 최고 {max(temps)}℃ / 최저 {min(temps)}℃")
        return rows

    @task
    def load(rows, ds=None):
        """L: CSV 파일로 저장"""
        os.makedirs(DATA_DIR, exist_ok=True)
        path = f"{DATA_DIR}/weather_{ds}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["time", "temperature", "humidity", "level"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"{len(rows)}건 저장 완료 → {path}")

    # 실행 순서: extract → transform → load
    load(transform(extract()))


weather_etl()