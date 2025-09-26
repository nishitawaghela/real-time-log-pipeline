import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

    # List of possible log levels and messages
LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
MESSAGES = [
    "User logged in successfully",
    "Failed to connect to database",
    "Payment processed",
    "API endpoint /api/users called",
    "Invalid credentials provided",
    "Data validation error on user input",
    "Cache cleared successfully"
    ]

def serializer(message):
        """JSON serializer for Kafka messages."""
        return json.dumps(message).encode('utf-8')

def create_producer():
        """Creates and returns a Kafka producer, retrying connection if necessary."""
        print("Attempting to connect to Kafka...")
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=['kafka:29092'],
                    value_serializer=serializer
                )
                print("Successfully connected to Kafka!")
                return producer
            except Exception as e:
                print(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)

def generate_log_message():
        """Generates a single random log message."""
        return {
            "timestamp": datetime.now().isoformat(),
            "level": random.choice(LOG_LEVELS),
            "message": random.choice(MESSAGES),
            "user_id": random.randint(100, 1000)
        }

if __name__ == "__main__":
        kafka_producer = create_producer()
        topic_name = 'logs'
        print(f"Started sending logs to Kafka topic: '{topic_name}'")

        while True:
            log_message = generate_log_message()
            print(f"Sending: {log_message}")
            kafka_producer.send(topic_name, value=log_message)
            time.sleep(random.uniform(0.5, 3)) # Wait for a random interval