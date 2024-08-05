from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import requests
import json
from confluent_kafka import Producer
import pendulum

# 상수 정의
KAFKA_TOPIC = 'topic_currPlayer'
KAFKA_BROKER = '172.31.5.148:9092'

# 게임의 Steam 앱 ID
GAMES = {
    '몬스터 헌터 월드': '582010',
    '엘든 링': '1245620',
    '팰월드': '1623730'
}

# Kafka Producer 설정
def create_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER
    })

def get_current_players(game_id):
    url = f'http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={game_id}'  # 공백 제거
    res = requests.get(url)

    if res.status_code == 200:
        data = res.json()
        return data.get('response', {}).get('player_count', 0)
    return 0

def get_steam_players_data(**context):
    producer = create_kafka_producer()
    local_tz = pendulum.timezone("Asia/Seoul")  # 한국 시간대 설정
    current_time = pendulum.now(local_tz).to_iso8601_string()  # 현재 시간을 ISO 8601 형식으로 가져옴

    for game_name, game_id in GAMES.items():
        player_count = get_current_players(game_id)
        key = game_name  # 게임 이름을 키로 사용
        value = json.dumps({
            'game': game_name,
            'player_count': player_count,
            'timestamp': current_time  # 현재 시간을 추가
        })  # 게임 이름, 플레이어 수, 타임스탬프를 JSON으로 변환
        
        # Kafka로 메시지 전송
        try:
            producer.produce(KAFKA_TOPIC, key=key, value=value)
        except Exception as e:
            print(f"Kafka 전송 실패: {e}")  # 에러 출력

    # 전송 완료 후 대기
    producer.flush()

@dag(
    start_date=datetime(2024, 7, 30, tzinfo=pendulum.timezone("Asia/Seoul")),  # 이 날짜가 현재보다 과거인지 확인
    schedule='*/1 * * * *',  # 매 30분마다 실행
    catchup=False
)
def steam_current_player_dag():
    get_current_player_task = PythonOperator(
        task_id='get_current_player_task',
        python_callable=get_steam_players_data
    )

    get_current_player_task  # DAG에 작업 추가

# DAG 정의
dag_instance = steam_current_player_dag()