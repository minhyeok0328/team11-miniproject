from pyspark.sql import SparkSession
from pyspark import SparkConf

from src.config import CONFIG


class Spark:
    def __init__(self, topic: str, app_name: str = 'Spark App') -> None:
        self.topic = topic
        self.app_name = app_name

    def create_spark_session(self):
        conf: SparkConf = SparkConf() \
                            .setAppName(self.app_name) \
                            .setMaster(f'spark://{CONFIG["SPARK_MASTER_IP"]}:7077') \
                            .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.elasticsearch:elasticsearch-spark-20_2.12:7.13.2,org.postgresql:postgresql:42.2.23')
        
        spark: SparkSession = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        # .option("kafka.bootstrap.servers", CONFIG['KAFKA_CLUSTER_IP']) \
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['KAFKA_CLUSTER_IP']) \
            .option("subscribe", self.topic) \
            .option("kafka.request.timeout.ms", "30000") \
            .option("kafka.consumer.fetch.max.wait.ms", "30000") \
            .option("kafka.max.poll.interval.ms", "300000") \
            .load()

        return df
