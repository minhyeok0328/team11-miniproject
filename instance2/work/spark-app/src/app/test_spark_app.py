from src.service import Spark
from pyspark.sql.functions import from_json, col, from_unixtime, date_format, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from src.config import CONFIG
from src.service import PostgreSQLHandler

TOPIC_NAME: str = 'steam_news'

class TestSparkApp:
    def __init__(self) -> None:
        self.df = Spark(topic=TOPIC_NAME, app_name='Test Spark App').create_spark_session()

        # 각 kafka 토픽 별로 spark에서 가공한 데이터를 저장할 예정입니다.
        self.postgres_handler = PostgreSQLHandler(table_name=TOPIC_NAME)

    def run(self):
        # 스팀 api에서 던져주는 json을 보고 스키마를 지정해 줍니다
        schema = StructType([
            StructField("appid", LongType(), True),
            StructField("newsitems", ArrayType(StructType([
                StructField("gid", StringType(), True),
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("is_external_url", StringType(), True),
                StructField("author", StringType(), True),
                StructField("contents", StringType(), True),
                StructField("feedlabel", StringType(), True),
                StructField("date", LongType(), True),
                StructField("feedname", StringType(), True),
                StructField("feed_type", LongType(), True),
                StructField("appid", LongType(), True)
            ])), True)
        ])

        json_df = self.df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

        # newsitems 배열을 개별 레코드로 분해
        exploded_df = json_df.withColumn("newsitem", explode(col("newsitems"))) \
            .select("appid", "newsitem.*")

        # date 컬럼을 원하는 형식으로 변환
        formatted_df = exploded_df.withColumn("date", date_format(from_unixtime(col("date")), "yyyy/MM-dd HH:mm:ss"))


        # Elastic Search에 연결한 뒤 insert 합니다
        es_query = formatted_df.writeStream \
            .outputMode("append") \
            .format("org.elasticsearch.spark.sql") \
            .option("checkpointLocation", "./checkpoint/dir/es") \
            .option("es.nodes", CONFIG["ELASTICSEARCH_HOST"]) \
            .option("es.port", CONFIG["ELASTICSEARCH_PORT"]) \
            .option("es.spark.sql.streaming.sink.log.enabled", "false") \
            .option("es.resource", f"{TOPIC_NAME}/doc") \
            .start()

        # PostgreSQL에도 동일한 데이터를 넣어줍니다
        pg_query = json_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", "./checkpoint/dir/pg") \
            .foreachBatch(self.write_to_postgres) \
            .start()

        es_query.awaitTermination()
        pg_query.awaitTermination()

    def write_to_postgres(self, batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{CONFIG['POSTGRES_HOST']}:{CONFIG['POSTGRES_PORT']}/{CONFIG['POSTGRES_DB']}") \
            .option("dbtable", TOPIC_NAME) \
            .option("user", CONFIG['POSTGRES_USER']) \
            .option("password", CONFIG['POSTGRES_PASSWORD']) \
            .mode("append") \
            .save()
