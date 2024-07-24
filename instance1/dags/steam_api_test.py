import datetime
import requests

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

XCOM_NEWS_KEY = 'news_data'

def get_steam_news_data(**context):
    # palworld news
    res = requests.get('https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?AppID=1623730')
    data = res.json()

    context['task_instance'].xcom_push(key=XCOM_NEWS_KEY, value=data)

def print_news_data(**context):
    news_data = context['task_instance'].xcom_pull(key=XCOM_NEWS_KEY, task_ids='print_news_data')
    print(f'news_data: {news_data}')

@dag(
    start_date=datetime.datetime(2025, 7, 24),
    schedule='*/5 * * * *',
    catchup=False
)
def steam_test_dags():
    get_steam_news_task = PythonOperator(
        task_id='get_steam_news_task',
        python_callable=get_steam_news_data,
        provide_context=True
    )

    print_news_task = PythonOperator(
        task_id='print_news_task',
        python_callable=print_news_data,
        provide_context=True
    )

    get_steam_news_task >> print_news_task

steam_test_dags()
