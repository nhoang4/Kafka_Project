from kafka import KafkaConsumer
import ssl
import json
import sys

# Configuration settings for the consumer
conf = {
    'bootstrap_servers': ["nhoang4-1.novalocal:9092", "nhoang4-2.novalocal:9092", "nhoang4-3.novalocal:9092", "nhoang4-4.novalocal:9092"],
    'sasl_plain_username': 'usercc',
    'sasl_plain_password': 'MyUserPasswd2024',
    'consumer_id': 'consumer_id'
}

# SSL context setup
context = ssl.create_default_context()
context.verify_mode = ssl.CERT_REQUIRED
context.load_verify_locations("cert.crt")

def consume_messages(topic_name):
    print('Starting consumer...')
    
    # Kafka consumer setup
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=conf['bootstrap_servers'],
                             group_id=conf['consumer_id'],
                             sasl_mechanism="PLAIN",
                             ssl_context=context,
                             security_protocol='SASL_SSL',
                             auto_offset_reset='earliest',
                             sasl_plain_username=conf['sasl_plain_username'],
                             sasl_plain_password=conf['sasl_plain_password'],
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    total_messages = 0
    sum_random_int = 0
    
    # Consume messages from the topic
    for message in consumer:
        total_messages += 1
        sum_random_int += message.value['random_int']
    
    print(f'Consumer finished. Total messages: {total_messages}, Sum of random_int: {sum_random_int}')
    consumer.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: consumer.py <topic_name>")
        sys.exit(1)

    topic_name = sys.argv[1]

    consume_messages(topic_name)

