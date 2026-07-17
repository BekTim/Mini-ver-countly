import time
import json
import random
import logging
from threading import Thread
from kafka import KafkaProducer, KafkaConsumer
import requests

# Configure logging to output directly to the Docker console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connection constants (inside the Docker network)
KAFKA_BROKER = 'kafka:9093'
CLICKHOUSE_URL = 'http://admin:strongpassword@clickhouse:8123/?database=analytics_db'
TOPIC = 'raw_events'

def setup_clickhouse():
    """Creates the events table in ClickHouse if it does not exist."""
    query = """
    CREATE TABLE IF NOT EXISTS events (
        user_id UInt32,
        action String,
        timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY timestamp;
    """
    
    logging.info("Attempting to connect to ClickHouse...")
    while True:
        try:
            response = requests.post(CLICKHOUSE_URL, data=query)
            
            # Verify that the database responded with a 200 (OK) status
            if response.status_code == 200:
                logging.info("Table 'events' in database 'analytics_db' is ready.")
                break
            else:
                logging.error(f"ClickHouse returned an error: {response.status_code} - {response.text}")
                time.sleep(2)
                
        except requests.exceptions.ConnectionError:
            logging.warning("ClickHouse is still starting, waiting 2 seconds...")
            time.sleep(2)

def generate_traffic(): #producer
    """Background thread: generates fake traffic and pushes it to Kafka."""
    logging.info("Traffic generator started...")
    
    # Wait a few seconds to ensure Kafka is fully ready
    time.sleep(5) 
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )   
    
    actions = ['click', 'view', 'purchase', 'login']
    
    while True:
        event = {"user_id": random.randint(1, 1000), "action": random.choice(actions)}
        try:
            producer.send(TOPIC, event)
        except Exception as e:
            logging.error(f"Error sending to Kafka: {e}")
            
        time.sleep(0.5) # 2 events per second

def consume_and_insert(): #consumer
    """Main process: reads from Kafka and batch inserts into ClickHouse."""
    logging.info("Consumer started, waiting for messages from Kafka...")
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    batch = []
    BATCH_SIZE = 10
    
    for msg in consumer:
        event = msg.value
        # Format string for SQL VALUES: (123, 'click')
        batch.append(f"({event['user_id']}, '{event['action']}')")
        
        if len(batch) >= BATCH_SIZE:
            # Combine array into a single bulk INSERT query
            query = f"INSERT INTO events (user_id, action) VALUES {','.join(batch)}"
            
            try:
                response = requests.post(CLICKHOUSE_URL, data=query)
                
                # Strict check: did ClickHouse actually write the data?
                if response.status_code == 200:
                    logging.info(f"Success! Batch of {BATCH_SIZE} events written to ClickHouse.")
                else:
                    logging.error(f"Error writing to ClickHouse: {response.text}")
            except Exception as e:
                logging.error(f"Network error when connecting to ClickHouse: {e}")
            
            # Always clear the batch (prevents memory leaks on persistent errors)
            batch = []

if __name__ == "__main__":
    # 1. Wait for and configure ClickHouse first
    setup_clickhouse()
    
    # 2. Start the traffic generator in a background thread
    Thread(target=generate_traffic, daemon=True).start()
    
    # 3. The consumer runs in the main thread, keeping the script alive
    consume_and_insert()