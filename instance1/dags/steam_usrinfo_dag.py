from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import requests
import json
from confluent_kafka import Producer
import pendulum
import redis

# 상수 정의
KAFKA_TOPIC = 'topic_GetUserInfo'
KAFKA_BROKER = '172.31.2.88:9092'  # Docker 환경에 맞게 수정
API_KEY = '1D6B63E8C3375FDCE46BD38620595819'  # 발급받은 Steam API 키
INITIAL_USER_ID = 76561197960434622  # 시작 Steam 사용자 ID

# Redis 설정
redis_client = redis.StrictRedis(host='172.19.0.2', port=6379, db=0)  # 도커 Redis IP 주소 사용

# Kafka Producer 설정
def create_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER
    })

def get_steam_players_data(**context):
    all_players = []
    last_user_id = redis_client.get('last_user_id')  # Redis에서 마지막 사용자 ID 가져오기
    last_user_id = int(last_user_id.decode()) if last_user_id else INITIAL_USER_ID  # bytes를 int로 변환
    player_count = 0  # 카운터 초기화
    max_players = 10  # 최대 플레이어 수 설정

    while player_count < max_players:
        url = f'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={API_KEY}&steamids={last_user_id}'
        res = requests.get(url)

        if res.status_code != 200:
            last_user_id -= 1  # 사용자 ID 감소
            continue  # 다음 사용자로 계속 진행

        data = res.json()
        players = data.get('response', {}).get('players', [])  # 플레이어 리스트 추출
        
        for player in players:
            all_players.append(player)  # 모든 플레이어 정보를 리스트에 추가
            player_count += 1  # 가져온 플레이어 수 카운트
            
            if player_count >= max_players:  # 최대 수에 도달하면 종료
                break

        last_user_id -= 1  # 사용자 ID 감소

    if all_players:
        # 마지막으로 불러온 사용자 ID를 Redis에 저장
        last_player_id = all_players[-1]['steamid']
        redis_client.set('last_user_id', last_player_id)  # Redis에 마지막 사용자 ID 저장

    context['task_instance'].xcom_push(key='players_data', value=json.dumps(all_players))  # JSON 문자열로 저장

def send_players_to_kafka(**context):  # 변환된 플레이어 데이터를 Kafka에 전송하는 함수
    producer = create_kafka_producer()
    
    # XCom에서 플레이어 데이터 가져오기
    data = context['task_instance'].xcom_pull(task_ids='get_steam_players_task', key='players_data')
    
    if data is None:
        return

    players = json.loads(data)  # XCom에서 가져온 JSON 문자열을 파싱

    for player in players:
        key = player['steamid']  # steamid를 키로 사용
        
        # Redis에서 중복 체크
        if redis_client.sismember('processed_user_ids', key): 
            continue  # 중복된 ID는 전송하지 않음
        
        value = json.dumps(player)  # 각 플레이어 정보를 JSON 문자열로 변환
        
        # Kafka로 메시지 전송
        try:
            producer.produce(KAFKA_TOPIC, key=key, value=value)
            redis_client.sadd('processed_user_ids', key)  # Redis에 전송한 ID 추가
        except Exception as e:
            print(f"Error producing message to Kafka: {e}")  # 에러 로그 출력

    # 전송 완료 후 대기
    producer.flush()

local_tz = pendulum.timezone("Asia/Seoul")

@dag(
    start_date=datetime(2024, 7, 30, tzinfo=local_tz),
    schedule='*/1 * * * *',  # 매 5분마다 실행
    catchup=False
)
def steam_usrinfo_dag():
    get_steam_players_task = PythonOperator(
        task_id='get_steam_players_task',
        python_callable=get_steam_players_data,
        provide_context=True,
    )

    send_players_to_kafka_task = PythonOperator(
        task_id='send_players_to_kafka_task',
        python_callable=send_players_to_kafka,
        provide_context=True,
    )

    get_steam_players_task >> send_players_to_kafka_task  # 의존성 설정

# DAG 정의
dag_instance = steam_usrinfo_dag()