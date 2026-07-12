import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Wait for Kafka broker to start up completely
def get_kafka_client():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=['kafka:29092'])
            return producer
        except NoBrokersAvailable:
            print("Waiting for Kafka to be available...")
            time.sleep(2)

# Produce a test message
producer = get_kafka_client()
print("Connected to Kafka! Sending message...")
producer.send('test-topic', b'Hello from Dockerized App!')
producer.flush()

# Consume the test message
print("Starting consumer...")
consumer = KafkaConsumer('test-topic', bootstrap_servers=['kafka:29092'], auto_offset_reset='earliest')
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
    break # Exit after receiving one message
