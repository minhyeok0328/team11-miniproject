CONFIG: dict = {
    'SPARK_VERSION': '3.5.1',
    'IP_ADDRESS': { # 인스턴스 재시작 할 때 마다 업데이트 해야 함
        'instance1': '172.31.2.88',
        'instance2': '172.31.5.148',
        'instance3': '172.31.9.18'
    },
    'KAFKA_CLUSTER_IP': lambda ip_dict: ','.join([f"{ip}:9092" for ip in ip_dict.values()]),
    'SPARK_MASTER_IP': '172.31.2.88',
    'POSTGRES_HOST': '172.31.5.148',
    'POSTGRES_PORT': 5432,
    'POSTGRES_DB': 'team11_db',
    'POSTGRES_USER': 'postgres',
    'POSTGRES_PASSWORD': 'postgres',
    'ELASTICSEARCH_HOST': '172.31.9.18',
    'ELASTICSEARCH_PORT': 9200
}

CONFIG['KAFKA_CLUSTER_IP'] = CONFIG['KAFKA_CLUSTER_IP'](CONFIG['IP_ADDRESS'])