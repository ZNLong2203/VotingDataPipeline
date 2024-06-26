import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from confluent_kafka import Consumer

spark_jars = ("{},{},{},{},{}".format(os.getcwd() + "spark_jars/spark_spark-sql-kafka-0-10_2.13-3.5.0.jar",
                                      os.getcwd() + "spark_jars/kafka-clients-3.5.0.jar",
                                      os.getcwd() + "spark_jars/spark-streaming-kafka-0-10-assembly_2.13-3.5.0.jar",
                                      os.getcwd() + "spark_jars/commons-pool2-2.12.0.jar",
                                      os.getcwd() + "spark_jars/spark-token-provider-kafka-0-10_2.13-3.5.0.jar"))

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("SparkStreaming") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config("spark.jars", "jars/postgresql-42.7.3.jar") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()

        schema = StructType([
            StructField("voter_id", StringType(), True),
            StructField("voter_name", StringType(), True),
            StructField("voter_age", IntegerType(), True),
            StructField("voter_gender", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("candidate_id", StringType(), True),
            StructField("candidate_name", StringType(), True),
            StructField("candidate_age", IntegerType(), True),
            StructField("candidate_gender", StringType(), True),
            StructField("party", StringType(), True),
            StructField("image_url", StringType(), True),
            StructField("voting_time", TimestampType(), True),
            StructField("vote", IntegerType(), True)
        ])

        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "voting_topic") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        kafka_df = kafka_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
            .withColumn('vote', col('vote').cast(IntegerType()))
        kafka_df = kafka_df.withWatermark("voting_time", "1 minute")

        # Using map_reduce to get the votes per candidate
        votes_per_candidate = kafka_df.groupBy("candidate_id", "candidate_name", "party").agg(
            F.sum("vote").alias("votes"))
        # votes_by_gender = kafka_df.groupBy("candidate_id", "candidate_name", "party", "voter_gender").count().alias("votes")

        # Create a temporary view to query the data
        kafka_df.createOrReplaceTempView("kafka_df_view")
        votes_by_gender = spark.sql("""
            SELECT candidate_id, candidate_name, party, voter_gender, COUNT(candidate_id) as votes
            FROM kafka_df_view
            GROUP BY candidate_id, candidate_name, party, voter_gender;
        """)

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
