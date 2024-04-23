import simplejson as json
import psycopg2
import random
from datetime import datetime
from confluent_kafka import Producer, SerializingProducer, Consumer, KafkaError, DeserializingConsumer

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

if __name__ == "__main__":
    try:
        # Connect to the PostgreSQL database
        postgres_conn = psycopg2.connect(
            host="localhost",
            database="voting",
            user="postgres",
            password="postgres"
        )

        cursor = postgres_conn.cursor()

        # Fetch candidates from the database
        candidates_query = cursor.execute(
            """
                SELECT row_to_json(col) 
                FROM (
                    SELECT * FROM candidates
                ) AS col;
            """
        )
        candidates = cursor.fetchall()
        candidates = [candidate[0] for candidate in candidates]

        if len(candidates) == 0:
            print("No candidates found")
        # print(candidates)

        # Configure Kafka consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'voting_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Disable auto commit
        })

        # Subscribe to the 'voters_topic'
        consumer.subscribe(['voters_topic'])

        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                print("No new messages")
                continue
            if msg.error():
                print(msg.error())
                break

            voter = msg.value().decode('utf-8')
            voter = json.loads(voter)

            chosen_candidate = random.choice(candidates)
            vote = voter | chosen_candidate | {
                "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "vote": 0
            }
            # print(vote)

            # Insert the vote into the database
            cursor.execute(
                """
                    INSERT INTO votes (candidate_id, voter_id, voting_time, vote)
                    VALUES (%s, %s, %s, %s);
                """, (vote["candidate_id"], vote["id"], vote["voting_time"], vote["vote"])
            )
            postgres_conn.commit()

            # Send the vote to Kafka
            producer = Producer({
                'bootstrap.servers': 'localhost:9092'
            })
            producer.produce(
                "voters_topic",
                key=vote["id"],
                value=json.dumps(vote),
                on_delivery=delivery_report
            )
            producer.flush()

    except Exception as e:
        print(e)
