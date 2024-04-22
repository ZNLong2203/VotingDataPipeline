import json
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
from confluent_kafka import Consumer

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("SparkStreaming") \
                .master("local[*]") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
                .config("spark.jars", "") \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()

        schema = StructType([

        ])

        kafka_df = spark \
                  .readStream("kafka") \
                  .option("kafka.bootstrap.servers", "localhost:9092") \
                  .option("subscribe", "votes_topic") \
                  .load() \
                  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                  .select(from_json("value", schema).alias("data")) \
                  .select("data.*")

        votes_per_candidate = kafka_df.groupBy("candidate_id", "candidate_name", "party").agg(F.sum("votes").alias("votes"))

        votes_by_gender = kafka_df.groupBy("gender").count().alias("votes")

        votes_per_candidate_to_kafka = votes_per_candidate \
                                      .selectExpr("to_json(struct(*)) AS value") \
                                      .writeStream \
                                      .format("kafka") \
                                      .option("kafka.bootstrap.servers", "localhost:9092") \
                                      .option("topic", "votes_per_candidate") \
                                      .option("checkpointLocation", "/tmp/checkpoint1") \
                                      .outputMode("update") \
                                      .start()

        votes_by_gender_to_kafka = votes_by_gender \
                                  .selectExpr("to_json(struct(*)) AS value") \
                                  .writeStream \
                                  .format("kafka") \
                                  .option("kafka.bootstrap.servers", "localhost:9092") \
                                  .option("topic", "votes_by_gender") \
                                  .option("checkpointLocation", "/tmp/checkpoint2") \
                                  .outputMode("update") \
                                  .start()

        # Await Termination for the streams
        votes_per_candidate_to_kafka.awaitTermination()
        votes_by_gender_to_kafka.awaitTermination()

    except Exception as e:
        print(e)