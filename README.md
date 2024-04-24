# Voting Data Pipeline

## Overview
This repository contains the code for a realtime election voting system. The system is built using Python, Kafka, Spark Streaming, Postgres and ElasticSearch. The system is built using Docker Compose to easily spin up the required services in Docker containers.

## System Architecture
![SystemArchitecture.png](img%2FSystemArchitecture.png)

## System Components
1. `data_api.py:` This is the Python script that creates the required tables on Postgres(Candidates, voters and votes), its also
create the Kafka topic and produce messages to the votes_topic.
2. `voting.py:` This is the Python script that contains the logics to consume the votes message from the Kafka topic (voters_topic), generate
voting data and produce a new topics name voting_topic on Kafka
3. `spark_streaming.py:` This is the Python script that contains the logic to consume the votes from the Kafka topic (voting_data), enrich the data from postgres and aggregate the votes and produce data to specific topics on Kafka.
4. `elastic_search.py` This is the Python script that transfers the data from kafka topics insert into ElasticSearch and visualize the data using ElasticVue
## Some commands interact with data in docker

1. Postgres:
   - `docker exec -it postgres psql -U postgres`
   - `\c voting`
   - `\d`
   - `SELECT * FROM votes;`
2. Kafka (broker image):
   - `docker exec -it broker kafka-topics --list --bootstrap-server broker:29092` List all topics
   - `kafka-console-consumer --topic voters_topic --bootstrap-server broker:29092` Consume messages from a topic
   - `kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic_name>` Delete a topic

## Data structure in kafka topics

### ElasticVue
![elastic_vue.png](img%2Felastic_vue.png)

### voters_topic
![voting_topic_data.png](img%2Fvoting_topic_data.png)

### voting_topic
![votes_topic_data.png](img%2Fvotes_topic_data.png)
