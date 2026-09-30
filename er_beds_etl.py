# ── 필요한 도구 불러오기 ──────────────────────────────
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET        # XML 응답 읽는 도구

import requests                            # API 요청 도구
from airflow.decorators import dag, task
from airflow.models import Variable       # Airflow에 저장한 API 키 꺼내는 도구
from airflow.providers.postgres.hooks.postgres import PostgresHook  # DB 접속 도구

API_URL = "http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire"
REGIONS = ["서울특별시", "경기도", "인천광역시", "부산광역시", "대구광역시"]  # 수집할 지역

CONN_ID = "my_postgres"                    # Airflow에 등록한 회사 DB 접속 정보
SCHEMA = '"GDB"'                           # 저장할 스키마 (대문자라 큰따옴표 필수)
TABLE = f"{SCHEMA}.er_beds_snapshot"       # 최종 테이블: "GDB".er_beds_snapshot


def to_int(value):
    """문자열 → 숫자 (빈 값이면 None)"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── DAG 정의 ──────────────────────────────────────────
@dag(
    dag_id="er_beds_etl",
    start_date=datetime(2026, 9, 1),
    schedule="0 * * * *",                  # 매시 정각 실행
    catchup=False,                         # 지난 시간은 몰아서 실행 안 함
    default_args={
        "retries": 2,                      # 실패 시 2번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도 간격 3분
    },
    tags=["medical", "etl"],
)
def er_beds_etl():

    # ── E: 지역별 수집 (지역 수만큼 태스크 자동 생성) ──
    @task
    def extract(region):
        params = {
            "serviceKey": Variable.get("data_go_kr_key"),  # 키는 Variable에서 꺼냄
            "STAGE1": region,              # 시/도 이름
            "pageNo": 1,
            "numOfRows": 500,              # 최대 500개 기관
        }
        res = requests.get(API_URL, params=params, timeout=30)
        res.raise_for_status()

        try:
            root = ET.fromstring(res.content)          # XML 읽기
        except ET.ParseError:
            raise ValueError(f"XML이 아닌 응답: {res.text[:300]}")
        if root.findtext(".//resultCode") != "00":     # 00 = 정상
            raise ValueError(f"API 오류 응답: {res.text[:300]}")

        items = []
        for it in root.iter("item"):                   # 병원 1곳 = item 1개
            items.append({
                "hpid": it.findtext("hpid"),           # 기관 ID
                "name": it.findtext("dutyName"),       # 병원 이름
                "tel": it.findtext("dutyTel3"),        # 응급실 전화
                "hvec": it.findtext("hvec"),           # 남은 병상
                "hvs01": it.findtext("hvs01"),         # 기준 병상
                "hvidate": it.findtext("hvidate"),     # 병원 입력 시각
                "region": region,
            })
        print(f"[{region}] {len(items)}개 기관 수집")
        return items

    # ── T: 지역 결과 합치기 + 정리 + 파생 컬럼 ──────────
    @task
    def transform(results, data_interval_end=None):
        # 수집 기준 시각 (한국 시간). 같은 실행을 다시 돌려도 같은 값 → 멱등성
        snapshot_at = data_interval_end.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for items in results:                          # 지역별 리스트를
            for it in items:                           # 하나로 펼치기
                available = to_int(it["hvec"])
                total = to_int(it["hvs01"])
                if not it["hpid"] or available is None:   # 필수값 없으면 제외
                    continue

                rate = round(available / total * 100, 1) if total else None  # 가용률(%)
                if available <= 0:
                    status = "포화"
                elif rate is not None and rate < 20:
                    status = "혼잡"
                else:
                    status = "여유"

                rows.append({
                    "snapshot_at": snapshot_at,
                    "hpid": it["hpid"],
                    "region": it["region"],
                    "name": it["name"],
                    "tel": it["tel"],
                    "available": available,
                    "total": total,
                    "available_rate": rate,
                    "status": status,
                    "updated_at": it["hvidate"],
                })
        print(f"정리 완료: {len(rows)}건 (기준 시각 {snapshot_at})")
        return rows

    # ── 품질 검사: 이상하면 여기서 멈춤 → DB 오염 방지 ──
    @task
    def check_quality(rows):
        if len(rows) == 0:
            raise ValueError("수집된 데이터가 0건이에요")
        duplicated = len(rows) - len({r["hpid"] for r in rows})
        if duplicated > 0:
            raise ValueError(f"같은 병원이 {duplicated}건 중복됐어요")
        print(f"품질 검사 통과: {len(rows)}건")
        return rows

    # ── L: 회사 DB에 적재 (같은 병원+같은 시각은 덮어쓰기) ──
    @task
    def load(rows):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                snapshot_at    TIMESTAMP,     -- 수집 기준 시각
                hpid           VARCHAR(20),   -- 기관 ID
                region         VARCHAR(20),   -- 시/도
                name           VARCHAR(100),  -- 병원 이름
                tel            VARCHAR(30),   -- 응급실 전화
                available      INT,           -- 남은 병상
                total          INT,           -- 기준 병상
                available_rate REAL,          -- 가용률(%)
                status         VARCHAR(10),   -- 포화/혼잡/여유
                updated_at     VARCHAR(20),   -- 병원 입력 시각
                PRIMARY KEY (hpid, snapshot_at)
            )
        """)
        fields = ["snapshot_at", "hpid", "region", "name", "tel",
                  "available", "total", "available_rate", "status", "updated_at"]
        hook.insert_rows(
            table=TABLE,
            rows=[tuple(r[f] for f in fields) for r in rows],
            target_fields=fields,
            replace=True,                          # 이미 있으면 덮어쓰기
            replace_index=["hpid", "snapshot_at"], # 중복 판단 기준
        )
        print(f"{len(rows)}건 적재 완료 → {TABLE}")

    # ── 리포트: 최신 시각 기준 지역별 요약 ──────────────
    @task
    def report():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        records = hook.get_records(f"""
            SELECT region,
                   COUNT(*)                                        AS hospitals,
                   SUM(available)                                  AS beds,
                   SUM(CASE WHEN status = '포화' THEN 1 ELSE 0 END) AS full_cnt
            FROM {TABLE}
            WHERE snapshot_at = (SELECT MAX(snapshot_at) FROM {TABLE})
            GROUP BY region
            ORDER BY beds
        """)
        print("지역 | 기관 수 | 남은 병상 합계 | 포화 기관 수")
        for region, hospitals, beds, full_cnt in records:
            print(f"{region} | {hospitals} | {beds} | {full_cnt}")

    # ── 실행 순서 ────────────────────────────────────
    raw = extract.expand(region=REGIONS)      # 지역 5개 → 태스크 5개 동시 실행
    rows = check_quality(transform(raw))
    load(rows) >> report()                    # 적재 끝나면 리포트


er_beds_etl()