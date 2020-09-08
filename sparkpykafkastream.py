from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("stedi-kafka").getOrCreate()
kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "Seans-MBP:9092") \
    .option("subscribe","redis-server")                  \
    .load()                                     

kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("CAST(value AS STRING)")

kafkaStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
#sqlStreamingDataFrame = kafkaStreamingDF            \
#    .writeStream                                        \
#    .queryName("values")                                \
#    .format("memory")                                   \
#    .start()

#redisEvents = spark.sql("Select * from values")
#redisEvents.show()