import time
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, BooleanType
import os
import traceback

current_dir = os.path.dirname(os.path.abspath(__file__))
temp_dir = os.path.join(current_dir, "temp")

spark = SparkSession.builder.appName("Sparky").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS btc")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS btc.trades (
        EventType STRING, 
        EventTime STRING, 
        Symbol STRING, 
        TradeId LONG, 
        Price DOUBLE, 
        Quantity DOUBLE, 
        BuyerOrderId LONG, 
        SellerOrderId LONG, 
        TradeTime LONG, 
        IsBuyerMaker BOOLEAN, 
        Ignore BOOLEAN
    )
"""
)

schema = StructType(
    [
        StructField("e", StringType()),
        StructField("E", LongType()),
        StructField("s", StringType()),
        StructField("t", LongType()),
        StructField("p", StringType()),
        StructField("q", StringType()),
        StructField("b", LongType()),
        StructField("a", LongType()),
        StructField("T", LongType()),
        StructField("m", BooleanType()),
        StructField("M", BooleanType()),
    ]
)

streaming = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "btc_trades")
    .load()
)

streaming = streaming.selectExpr("CAST(value AS STRING)")


def write_stream_to_file(streaming, query_name):
    query = (
        streaming.writeStream.outputMode("append")
        .format("json")
        .option("path", temp_dir)
        .option("checkpointLocation", f"{temp_dir}/checkpoint")
        .queryName(query_name)
        .start()
    )
    query.awaitTermination()

def read_file_to_hive(spark, input_dir, table_name):
    json_files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    for json_file in json_files:
        try:
            df = spark.read.json(os.path.join(input_dir, json_file))

            df = df.withColumn("parsed_value", from_json(df["value"], schema))

            for field in schema.fields:
                if df.columns.count(field.name) == 0:
                    df = df.withColumn(field.name, col("parsed_value." + field.name))

            df = df.drop("value").drop("parsed_value")

            if not df.rdd.isEmpty():
                df.write.insertInto(table_name)

                os.remove(os.path.join(input_dir, json_file))
                crc_file = f".{json_file}.crc"
                if os.path.exists(os.path.join(input_dir, crc_file)):
                    os.remove(os.path.join(input_dir, crc_file))
        except Exception as e:
            print(f"Error processing file {json_file}: {e}")
            traceback.print_exc()

        time.sleep(0.1)


t1 = threading.Thread(target=write_stream_to_file, args=(streaming, "streaming"))
t2 = threading.Thread(target=read_file_to_hive, args=(spark, temp_dir, "btc.trades"))
t1.start()
time.sleep(1)
t2.start()
