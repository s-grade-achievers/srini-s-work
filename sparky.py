from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sparky").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
streaming = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "btc_trades").load()
streaming = streaming.selectExpr("CAST(value AS STRING)")
query = streaming.writeStream.outputMode("append").format("console").start()
query.awaitTermination()