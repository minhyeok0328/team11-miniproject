from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import requests
from confluent_kafka import Producer
import json
import redis

# Redis 설정
redis_client = redis.StrictRedis(host='172.19.0.2', port=6379, db=0)  # airflow-redis-1 docker 컨테이너 IP, db=0은 기본 DB에 연결함


KAFKA_TOPIC = 'rct_plyd_games'
KAFKA_BROKER = '172.31.2.88:9092'
API_KEY = '1D6B63E8C3375FDCE46BD38620595819'
START_STEAM_ID = 76561197960499900 


def create_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'kafkaproducer-rct_plyd_games'
    })

# Redis에 존재하지 않을 때
def get_recently_played_games(**kwargs):
    try:
        last_steam_id = redis_client.get('last_steam_id')
        last_steam_id = int(last_steam_id.decode('utf-8')) if last_steam_id else START_STEAM_ID
    except Exception as e:
        print(f"Redis에서 가져오는 중 오류 발생: {str(e)}")
        last_steam_id = START_STEAM_ID

    
    recently_played_games = []
    steam_id = last_steam_id

    for _ in range(100):
        url = f'http://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key={API_KEY}&steamid={steam_id}'
        print(f"요청할 URL: {url}")
        res = requests.get(url)
        if res.status_code != 200:
            print(f"{steam_id}의 최근 플레이된 게임을 가져오는 데 실패했습니다: {res.status_code}, 응답: {res.text}")
            steam_id -= 1
            continue

        data = res.json()
        games = data.get('response', {}).get('games', [])
        
        
        for game in games:
            game['steam_id'] = steam_id
        if games:
            recently_played_games.extend(games)
            print(f"{steam_id}의 최근 플레이된 게임 데이터 가져오기 완료: {games}")
        else:
            print(f"{steam_id}는 플레이한 게임이 없습니다. 다음 사용자로 진행합니다.")
        
        steam_id -= 1

    
    kwargs['task_instance'].xcom_push(key='recently_played_games', value=recently_played_games)
    redis_client.set('last_steam_id', steam_id)  


def send_data_to_kafka(**kwargs):
    producer = create_kafka_producer()

    task_instance = kwargs['task_instance']
    recently_played_games = task_instance.xcom_pull(task_ids='task1_get_recently_played_games', key='recently_played_games')

    if recently_played_games is None or not recently_played_games:
        print("Kafka에 보낼 데이터가 없습니다.")
        return

    for game in recently_played_games:
        key = str(game['appid']).encode('utf-8')
        value = json.dumps(game).encode('utf-8')
        try:
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=delivery_report)
            print(f"Kafka에 최근 플레이된 게임 데이터 전송 완료: {value}")
        except Exception as e:
            print(f"Kafka에 메시지 전송 실패: {str(e)}")
    producer.flush()


def delivery_report(err, msg):
    if err is not None:
        print(f"메시지 전달 실패: {err}")
    else:
        print(f"메시지가 {msg.topic()} [{msg.partition()}]에 전달되었습니다. 키: {msg.key().decode('utf-8')}")


dag = DAG(
    dag_id='steam_get_recently_played_games',
    start_date=datetime(2024, 7, 29, tzinfo=pendulum.timezone("Asia/Seoul")),
    schedule_interval='*/1 * * * *',  
    catchup=False,
)


task1_get_recently_played_games = PythonOperator(
    task_id='task1_get_recently_played_games',
    python_callable=get_recently_played_games,
    provide_context=True,
    dag=dag
)

task2_send_data_to_kafka = PythonOperator(
    task_id='task2_send_data_to_kafka',
    python_callable=send_data_to_kafka,
    provide_context=True,
    dag=dag
)


task1_get_recently_played_games >> task2_send_data_to_kafka