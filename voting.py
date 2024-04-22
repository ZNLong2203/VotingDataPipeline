import simplejson as json
import psycopg2
import random
from datetime import datetime
from confluent_kafka import Producer, SerializingProducer, Consumer, KafkaError

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

        # Set up Kafka consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'voting_group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(['voters_topic'])
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            print("No messages found in the topic")
        elif msg.error():
            print(msg.error())
        else:
            # Parse voter information from Kafka message
            voter = json.loads(msg.value().decode('utf-8'))

            # Choose a random candidate
            chosen_candidate = random.choice(candidates)

            # Create a vote record
            vote = {
                "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "voter_id": voter["voter_id"],
                "candidate_id": chosen_candidate["candidate_id"],
                "vote": 0
            }

            # Insert the vote record into the database
            cursor.execute(
                """
                    INSERT INTO votes (candidate_id, voter_id, voting_time, vote)
                    VALUES (%s, %s, %s, %s);
                """, (vote["candidate_id"], vote["voter_id"], vote["voting_time"], vote["vote"])
            )

    except Exception as e:
        print(e)
