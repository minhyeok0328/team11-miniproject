from src.spark import Spark
from src.config import SPARK_VERSION

def main() -> None:
    spark_test = Spark(topic='test_topic')

if __name__ == '__main__':
    main()
