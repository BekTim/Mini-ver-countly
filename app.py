import time
import json
import random
import logging
from threading import Thread
from kafka import KafkaProducer, KafkaConsumer
import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

KAFKA_BROKER = 'kafka:9093'
CLICKHOUSE_URL = 'http://clickhouse:8123'
TOPIC = 'raw_events'

# 1. Готовим ClickHouse
def setup_clickhouse():
    query = """
    CREATE TABLE IF NOT EXISTS events (
        user_id UInt32, action String, timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY timestamp;
    """
    while True:
        try:
            requests.post(CLICKHOUSE_URL, data=query)
            logging.info("ClickHouse готов к работе.")
            break
        except requests.exceptions.ConnectionError:
            logging.warning("Жду ClickHouse...")
            time.sleep(2)

# 2. Продюсер (Генерирует мусорный трафик)
def generate_traffic():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    actions = ['click', 'view', 'purchase', 'login']
    while True:
        event = {"user_id": random.randint(1, 1000), "action": random.choice(actions)}
        producer.send(TOPIC, event)
        time.sleep(0.5) # Кидаем по 2 ивента в секунду

# 3. Консьюмер (Забирает из Kafka и пишет в ClickHouse батчами)
def consume_and_insert():
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    batch = []
    for msg in consumer:
        event = msg.value
        batch.append(f"({event['user_id']}, '{event['action']}')")
        
        # Набрали 10 штук — пушим в БД
        if len(batch) >= 10:
            query = f"INSERT INTO events (user_id, action) VALUES {','.join(batch)}"
            requests.post(CLICKHOUSE_URL, data=query)
            logging.info(f"Батч из 10 ивентов улетел в ClickHouse")
            batch = []

if __name__ == "__main__":
    setup_clickhouse()
    
    # Запускаем генератор в фоновом потоке
    Thread(target=generate_traffic, daemon=True).start()
    
    # Консьюмер работает в основном потоке
    consume_and_insert()