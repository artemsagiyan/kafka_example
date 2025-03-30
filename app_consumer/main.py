from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
import json
from datetime import datetime
from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI

# Конфигурация
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'errors'
CONSUMER_GROUP_ID = 'error_consumers'  # Добавлен group_id

PG_HOST = 'localhost'
PG_DATABASE = 'postgres_db'
PG_PORT = 5430
PG_USER = 'postgres_user'
PG_PASSWORD = 'postgres_password'

# Глобальные переменные для ресурсов
consumer = None
pg_conn = None
consumer_task = None


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


async def consume_errors():
    global pg_conn, consumer

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

    create_table_if_not_exists(pg_conn)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP_ID,  # Добавлен group_id
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Отключаем авто-коммит
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consumer started. Listening to topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            error_data = message.value
            print(f"Received error: {error_data}")

            try:
                insert_error(pg_conn, error_data)
                consumer.commit()  # Теперь commit будет работать
                print("Error saved to PostgreSQL")
            except Exception as e:
                print(f"Failed to save error to PostgreSQL: {e}")
                pg_conn.rollback()
    except Exception as e:
        print(f"Consumer error: {e}")
    finally:
        if consumer:
            consumer.close()
        if pg_conn:
            pg_conn.close()


app = FastAPI()


@app.get("/upload_errors")
async def root():
    await consume_errors()
    return {"message": "Kafka Consumer Service"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)