from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
import json
from datetime import datetime


KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'errors'


PG_HOST = 'localhost'
PG_DATABASE = 'postgres_db'
PG_PORT = 5430
PG_USER = 'postgres_user'
PG_PASSWORD = 'postgres_password'


def create_table_if_not_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS errors (
                id SERIAL PRIMARY KEY,
                time TIMESTAMP NOT NULL,
                code INTEGER NOT NULL,
                message TEXT,
                details TEXT
            )
        """)
        conn.commit()


def insert_error(conn, error_data):
    query = sql.SQL("""
        INSERT INTO errors (time, code, message, details)
        VALUES (%s, %s, %s, %s)
    """)

    with conn.cursor() as cursor:
        cursor.execute(query, (
            datetime.now(),
            error_data.get('code'),
            error_data.get('message'),
            error_data.get('details')
        ))
        conn.commit()


def consume_errors():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

    # Создаем таблицу, если ее нет
    create_table_if_not_exists(conn)

    # Настраиваем Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print(f"Consumer started. Listening to topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            error_data = message.value
            print(f"Received error: {error_data}")

            try:
                insert_error(conn, error_data)
                print("Error saved to PostgreSQL")
            except Exception as e:
                print(f"Failed to save error to PostgreSQL: {e}")
                conn.rollback()

    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    consume_errors()