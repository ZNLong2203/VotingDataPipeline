from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import simplejson as json

if __name__ == "__main__":
    try:
        # Elasticsearch connection
        es = Elasticsearch([{
            'host': 'localhost',
            'port': 9200,
            'scheme': 'http'
        }])

        # Kafka consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'elastic_search_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false'
        })
        consumer.subscribe(["votes_per_candidate"])

        # Poll for new messages
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                print("No new messages")
                continue
            if msg.error():
                print(msg.error())
                break

            # Convert the message to a dictionary
            message_data = json.loads(msg.value().decode('utf-8'))

            # Insert the data into Elasticsearch
            es.index(index='votes', id="Voting_Pipeline", body=message_data)

    except Exception as e:
        print(e)