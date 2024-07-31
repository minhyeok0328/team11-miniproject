import psycopg2
from src.config import CONFIG

class PostgreSQLHandler:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.postgres_url = f"jdbc:postgresql://{CONFIG['POSTGRES_HOST']}:{CONFIG['POSTGRES_PORT']}/{CONFIG['POSTGRES_DB']}"
        self.postgres_properties = {
            "user": CONFIG['POSTGRES_USER'],
            "password": CONFIG['POSTGRES_PASSWORD']
        }

    def create_table_if_not_exists(self):
        conn = psycopg2.connect(
            dbname=CONFIG['POSTGRES_DB'],
            user=CONFIG['POSTGRES_USER'],
            password=CONFIG['POSTGRES_PASSWORD'],
            host=CONFIG['POSTGRES_HOST'],
            port=CONFIG['POSTGRES_PORT']
        )
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            idx SERIAL PRIMARY KEY,
            items JSONB
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()

    def write_to_postgres(self, batch_df, batch_id):
        self.create_table_if_not_exists()
        batch_df.write \
            .format("jdbc") \
            .option("url", self.postgres_url) \
            .option("dbtable", self.table_name) \
            .option("user", CONFIG['POSTGRES_USER']) \
            .option("password", CONFIG['POSTGRES_PASSWORD']) \
            .mode("append") \
            .save()
