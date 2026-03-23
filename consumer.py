from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import sys

def demo_consumer():
    try:
        # Initializing the Kafka consumer
        consumer = KafkaConsumer(
            'demo_topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest', # Read from the beginning of the topic
            enable_auto_commit=True,
            group_id='demo-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=5000, # Exit after 5 seconds of inactivity
            api_version=(0, 10, 1)
        )
        print("[SUCCESS] Consumer connected to Kafka. Listening to 'demo_topic'...")
        
        count = 0
        for message in consumer:
            print(f"Received -> Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
            count += 1
            
        consumer.close()
        
        if count == 0:
            print("[WARNING] No messages were found in the topic within the timeout period.")
        else:
            print(f"[SUCCESS] Finished consuming {count} messages.")
            
    except NoBrokersAvailable:
        print("[ERROR] Could not connect to any Kafka broker. Ensure Kafka is running on 'localhost:9092'.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error while consuming: {e}")

if __name__ == "__main__":
    demo_consumer()
