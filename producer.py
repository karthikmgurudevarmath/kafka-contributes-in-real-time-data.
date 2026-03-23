from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import sys

def demo_producer():
    try:
        # Initializing the Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1) # Fallback to avoid some timeout issues with newer brokers
        )
        print("[SUCCESS] Producer connected to Kafka.")
        
        # Sending 5 messages to the 'demo_topic'
        for i in range(1, 6):
            data = {'message_id': i, 'content': f'Hello Kafka from Producer! {i}'}
            future = producer.send('demo_topic', value=data)
            record_metadata = future.get(timeout=10)
            
            print(f"[{time.strftime('%H:%M:%S')}] Sent message {i} to topic '{record_metadata.topic}' (partition {record_metadata.partition}, offset {record_metadata.offset})")
            time.sleep(1)
            
        producer.flush()
        producer.close()
        print("[SUCCESS] Finished producing messages.")
        
    except NoBrokersAvailable:
        print("[ERROR] Could not connect to any Kafka broker. Ensure Kafka is running on 'localhost:9092'.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error while producing: {e}")

if __name__ == "__main__":
    demo_producer()
