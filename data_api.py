import requests
import json
import psycopg2
from datetime import datetime
from confluent_kafka import Producer, SerializingProducer

URL = "https://randomuser.me/api/"
PARTIES = ["Republican", "Democrat", "Independent"]
def create_table(conn, cursor):
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS candidates (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                gender VARCHAR(255),
                party VARCHAR(255),
                image_url TEXT
            );
        """
    )

    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS voters (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                gender VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(255),
                address VARCHAR(255)
            );
        """
    )

    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS votes (
                candidate_id VARCHAR(255) REFERENCES candidates(id) UNIQUE,
                voter_id VARCHAR(255) REFERENCES voters(id) UNIQUE,
                voting_time TIMESTAMP,
                vote INTEGER DEFAULT 0
            );
        """
    )

    conn.commit()

def generate_candidate(cursor, i):
    response = requests.get(URL)

    if response.status_code == 200:
        response = response.json()["results"][0]

        # Inserting data into the candidates table
        id = response["login"]["uuid"]
        name = f"{response['name']['first']} {response['name']['last']}"
        age = response["dob"]["age"]
        gender = response["gender"]
        party = PARTIES[i]
        image_url = response["picture"]["large"]

        cursor.execute("""
            INSERT INTO candidates (id, name, age, gender, party, image_url)
            VALUES(%s, %s, %s, %s, %s, %s);
            """, (id, name, age, gender, party, image_url)
        )
    else:
        print("Error: ", response.status_code)

def generate_voter(cursor):
    response = requests.get(URL)

    if response.status_code == 200:
        response = response.json()["results"][0]

        # Inserting data into the voters table
        voter_data = {
            "id": response["login"]["uuid"],
            "name": f"{response['name']['first']} {response['name']['last']}",
            "age": response["dob"]["age"],
            "gender": response["gender"],
            "email": response["email"],
            "phone": response["phone"],
            "address": f"{response['location']['street']['number']} {response['location']['street']['name']}, {response['location']['city']}, {response['location']['state']}, {response['location']['country']}"
        }

        cursor.execute("""
            INSERT INTO voters (id, name, age, gender, email, phone, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (voter_data["id"], voter_data["name"], voter_data["age"], voter_data["gender"], voter_data["email"], voter_data["phone"], voter_data["address"])
        )
        postgres_conn.commit()

        # Sending data to Kafka Producer
        producer = Producer({
            'bootstrap.servers': 'localhost:9092'
        })
        producer.produce("voters_topic", key=voter_data['id'], value=json.dumps(voter_data))
        producer.flush()
    else:
        print("Error: ", response.status_code)

if __name__ == "__main__":
    try:
        postgres_conn = psycopg2.connect(
            host="localhost",
            database="voting",
            user="postgres",
            password="postgres"
        )
        postgres_cursor = postgres_conn.cursor()

        # Creating tables
        create_table(postgres_conn, postgres_cursor)

        # Inserting data into the candidates table if not have any data
        postgres_cursor.execute("""
            SELECT * FROM candidates;
        """)
        if not postgres_cursor.fetchall():
            for i in range(3):
                generate_candidate(postgres_cursor, i)
                postgres_conn.commit()

        # Inserting data voter into the voters table and sending to Kafka
        for i in range(1000):
            generate_voter(postgres_cursor)

    except Exception as e:
        print(e)
