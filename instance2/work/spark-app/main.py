from src.app import TestSparkApp

def main() -> None:
    spark_app: list = [TestSparkApp]
    
    for app in spark_app:
        app().run()

if __name__ == '__main__':
    main()
