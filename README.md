# Voting Data Pipeline

## Overview
This repository contains the code for a realtime election voting system. The system is built using Python, Kafka, Spark Streaming, Postgres and ElasticSearch. The system is built using Docker Compose to easily spin up the required services in Docker containers.

## System Architecture

## System Components

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
![voting_topic_data.png](img%2Fvoting_topic_data.png)