from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# 텍스트 파일 읽기
text_file = spark.read.text("/spark-app/src/news_data/news.txt")

# 각 라인을 단어로 분리
words = text_file.select(explode(split(text_file.value, " ")).alias("word"))

# 각 단어의 빈도 계산
word_counts = words.groupBy("word").count()

# 결과 출력
word_counts.show()

# 결과를 파일로 저장
word_counts.write.csv("/spark-app/src/output")

# Spark 세션 종료
spark.stop()
