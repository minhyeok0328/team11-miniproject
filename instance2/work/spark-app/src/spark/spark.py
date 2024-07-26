import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from src.config import KAFKA_CONFIG
from pyspark.sql.functions import explode, split


class Spark:
    def __init__(self, topic) -> None:
        conf: SparkConf = SparkConf() \
            .setAppName("spark preprocessing pipeline") \
            .setMaster("spark://3.39.76.12:7077") \
            .set("spark.es.nodes", "13.125.166.88") \
            .set("spark.es.port", "9200") \
            .set("spark.es.nodes.wan.only", "true") \
            .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
            .set("spark.dynamicAllocation.enabled", "false") \
            .set("spark.shuffle.service.enabled", "false") \
            .set("spark.executor.memory", "4g") \
            .set("spark.executor.cores", "2") \
            .set("spark.driver.memory", "4g")

        self.spark: SparkSession = SparkSession \
                    .builder \
                    .config(conf=conf) \
                    .getOrCreate()

        self.df = self.spark \
                    .readStream \
                    .format('kafka') \
                    .option('kafka.bootstrap.servers', '3.39.76.12:9092,54.180.245.105:9092,13.125.166.88:9092') \
                    .option('subscribe', topic) \
                    .option("failOnDataLoss","False") \
                    .option('startingOffsets', 'latest') \
                    .load()

        text_file = self.spark.read.text("/spark-app/src/spark/news.txt")

        words = text_file.select(explode(split(text_file.value, " ")).alias("word"))

        word_counts = words.groupBy("word").count()
        word_counts.write.csv("/spark-app/src/output")

        self.spark.stop()
