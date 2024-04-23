import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from confluent_kafka import Consumer

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("SparkStreaming") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config("spark.jars", "jars/postgresql-42.7.3.jar") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()

        schema = StructType([
            StructField("voter_id", StringType()),
            StructField("voter_name", StringType()),
            StructField("voter_age", IntegerType()),
            StructField("voter_gender", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("address", StringType()),
            StructField("candidate_id", StringType()),
            StructField("candidate_name", StringType()),
            StructField("candidate_age", IntegerType()),
            StructField("candidate_gender", StringType()),
            StructField("party", StringType()),
            StructField("image_url", StringType()),
            StructField("voting_time", TimestampType()),
            StructField("votes", IntegerType())
        ])

        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "voting_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(from_json(col("CAST(value AS STRING)"), schema).alias("data")) \
            .select("data")

        kafka_df = kafka_df.withColumn("voting_time", col("data.voting_time").cast(TimestampType())) \
            .withColumn('vote', col('data.vote').cast(IntegerType()))
        kafka_df = kafka_df.withWatermark("voting_time", "1 minute")

        votes_per_candidate = kafka_df.groupBy("candidate_id", "candidate_name", "party").agg(
            F.sum("vote").alias("votes"))

        votes_by_gender = kafka_df.groupBy("voter_gender").count().alias("votes")

        votes_per_candidate_to_kafka = votes_per_candidate \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "votes_per_candidate") \
            .option("checkpointLocation", "tmp/checkpoint1") \
            .outputMode("update") \
            .start()

        votes_by_gender_to_kafka = votes_by_gender \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "votes_by_gender") \
            .option("checkpointLocation", "tmp/checkpoint2") \
            .outputMode("update") \
            .start()

        # Await Termination for the streams
        votes_per_candidate_to_kafka.awaitTermination()
        votes_by_gender_to_kafka.awaitTermination()

    except Exception as e:
        print(e)
