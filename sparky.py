import time
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, BooleanType
import os
import json
import traceback

current_dir = os.path.dirname(os.path.abspath(__file__))
temp_dir = os.path.join(current_dir, "srini_s_warriors")

spark = SparkSession.builder.appName("Sparky").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS btc")
if not spark.catalog.tableExists("btc.srini_s_warriors"):
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS btc.srini_s_warriors (
            e1 STRING, 
            E STRING, 
            s STRING, 
            t1 LONG, 
            p STRING, 
            q STRING, 
            b LONG, 
            a LONG, 
            T LONG, 
            m1 BOOLEAN, 
            M BOOLEAN
        )
    """
    )

schema = StructType(
    [
        StructField("e1", StringType()),
        StructField("E", LongType()),
        StructField("s", StringType()),
        StructField("t1", LongType()),
        StructField("p", StringType()),
        StructField("q", StringType()),
        StructField("b", LongType()),
        StructField("a", LongType()),
        StructField("T", LongType()),
        StructField("m1", BooleanType()),
        StructField("M", BooleanType()),
    ]
)

streaming = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "srini-s-warriors")
    .load()
)

streaming = streaming.selectExpr("CAST(value AS STRING)")


def write_stream_to_file(streaming, query_name):
    query = (
        streaming.writeStream.outputMode("append")
        .format("text")
        .option("path", temp_dir)
        .option("checkpointLocation", f"{temp_dir}/checkpoint")
        .queryName(query_name)
        .start()
    )
    query.awaitTermination()


def read_txt(file):
    with open(file, "r") as f:
        f = f.readlines()
        f = [i[2:-2] for i in f]
        f = [i.split(",") for i in f]
        f = [[item.replace("\\", "") for item in sublist] for sublist in f]
        result = []
        for item in f:
            item[-1] = item[-1][:-1]
            item = [
                (
                    x.replace('e":', 'e1":')
                    if 'e":' in x
                    else (
                        x.replace('m":', 'm1":')
                        if 'm":' in x
                        else x.replace('t":', 't1":') if 't":' in x else x
                    )
                )
                for x in item
            ]
            json_str = "{" + ", ".join(item) + "}"
            try:
                result.append(json.loads(json_str))
            except Exception as e:
                print(f"Error decoding JSON: {e}")
        return result


def read_file_to_hive(spark, input_dir):
    files = [f for f in os.listdir(input_dir) if f.endswith(".txt")]
    for file in files:
        print("team srini-s-warriors added file", file, "to hive")
        try:
            dict_list = read_txt(os.path.join(input_dir, file))
            if dict_list:
                df = spark.createDataFrame(dict_list)
                df.write.mode("append").format("hive").saveAsTable("btc.srini_s_warriors")

            os.remove(os.path.join(input_dir, file))
            crc_file = f".{file}.crc"
            if os.path.exists(os.path.join(input_dir, crc_file)):
                os.remove(os.path.join(input_dir, crc_file))
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            traceback.print_exc()

t1 = threading.Thread(target=write_stream_to_file, args=(streaming, "streaming"))
t2 = threading.Thread(target=read_file_to_hive, args=(spark, temp_dir))
t1.start()
time.sleep(1)
t2.start()
