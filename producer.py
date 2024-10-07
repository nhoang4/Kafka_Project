from kafka import KafkaProducer
import ssl
import json
import time
import random
import sys

# Configuration settings for the producer
conf = {
    'bootstrap_servers': ["nhoang4-1.novalocal:9092", "nhoang4-2.novalocal:9092", "nhoang4-3.novalocal:9092", "nhoang4-4.novalocal:9092"],
    'sasl_plain_username': 'usercc',
    'sasl_plain_password': 'MyUserPasswd2024'
}

# SSL context setup
context = ssl.create_default_context()
context.verify_mode = ssl.CERT_REQUIRED
context.load_verify_locations("cert.crt")

def produce_messages(topic_name, num_messages):
    print('Starting producer...')
    
    # Kafka producer setup
    producer = KafkaProducer(bootstrap_servers=conf['bootstrap_servers'],
                             sasl_mechanism="PLAIN",
                             ssl_context=context,
                             security_protocol='SASL_SSL',
                             sasl_plain_username=conf['sasl_plain_username'],
                             sasl_plain_password=conf['sasl_plain_password'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for message_id in range(1, num_messages + 1):
        # Generate a message with id, random integer, and timestamp
        message = {
            'id': message_id,
            'random_int': random.randint(1, 100),
            'timestamp': time.time()
        }
        # Send the message to the topic
        producer.send(topic_name, value=message)
        producer.flush()

    producer.close()
    print(f'Producer finished. Last message id was {message_id}.')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: producer.py <topic_name> <num_messages>")
        sys.exit(1)

    topic_name = sys.argv[1]
    num_messages = int(sys.argv[2])

    produce_messages(topic_name, num_messages)

