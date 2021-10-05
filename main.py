import os
import time
import sys
from random import choice
from string import ascii_lowercase
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import MessageSizeTooLargeError


def main():
    producer = KafkaProducer(
        bootstrap_servers=os.environ['KAFKA_BROKERS'],
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_password='mysecret',
        sasl_plain_username='producer',
        ssl_cafile='CA.pem',
        api_version=(0, 10),
        acks="all",
        client_id="dianet",
        value_serializer=None,
        max_request_size=104857600,
    )

    idx = 0
    while True:
        string_val = "".join(choice(ascii_lowercase) for _ in range(1000))
        data = bytes(string_val * 20000, 'utf-8')
        idx += 1
        print(f'sending message {idx} of size {len(data)}')
        try:
            future = producer.send('topic1', key=bytes(f'{idx}', 'utf-8'), value=data)
            future.get(timeout=5)
            print('produced next message')
        except MessageSizeTooLargeError as ex:
            print(f'failed to produce: {ex.message}')

        time.sleep(1)


def consume():
    consumer = KafkaConsumer(
        'topic1',
        bootstrap_servers=os.environ['KAFKA_BROKERS'],
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_password='mysecret',
        sasl_plain_username='consumer',
        ssl_cafile='CA.pem',
        api_version=(0, 10),
        group_id='dianet',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        max_partition_fetch_bytes=100*1024*1024,
    )

    while True:
        msg_pack = consumer.poll(timeout_ms=1000, max_records=1, update_offsets=True)
        if msg_pack:
            for partition, records in msg_pack.items():
                keys = [r.key.decode('utf-8') for r in records]
                print(f"consumed {len(records)} from {partition}: {''.join(keys)}")
        else:
            print(f'no mesage: {msg_pack}')
        time.sleep(0.1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'consume':
        consume()
    else:
        main()
