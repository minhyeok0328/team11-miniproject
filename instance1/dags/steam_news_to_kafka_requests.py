from datetime import datetime, timedelta
from airflow.decorators import dag, task
from confluent_kafka import Producer
import requests
import json

@dag(
    start_date=datetime(2023, 7, 25),
    schedule_interval=timedelta(hours=24                               ),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def steam_news_to_kafka_requests():
    APP_ID = 1623730

    @task
    def extract_news():
        """Steam News API에서 뉴스 데이터를 가져오는 함수"""
        url = f'https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid={APP_ID}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['appnews']['newsitems']

    @task
    def transform_news(news_data):
        """뉴스 데이터를 Kafka 메시지 형태로 변환하는 함수"""
        return [
            {
                'gid': news['gid'],
                'title': news['title'],
                'url': news['url'],
                'contents': news['contents'],
                'feedlabel': news['feedlabel'],
                'date': news['date'],
            }
            for news in news_data
        ]

    @task
    def send_to_kafka(transformed_news):
        """변환된 뉴스 데이터를 Kafka에 전송하는 함수"""
        producer_config = {
            'bootstrap.servers': "172.31.2.88:9092",  # Kafka 브로커 주소
        }
        producer = Producer(producer_config)

        for news in transformed_news:
            producer.produce('steam_news', value=json.dumps(news))
            producer.flush()

    news_data = extract_news()
    transformed_news = transform_news(news_data)
    send_to_kafka(transformed_news)

steam_news_to_kafka_requests()