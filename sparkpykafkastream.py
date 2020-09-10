from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
schema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]


)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("stedi-kafka").getOrCreate()
kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "Seans-MBP:9092") \
    .option("subscribe","redis-server")                  \
    .load()                                     

# this is necessary for Kafka Data Frame to be readable, into a single column  value
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("CAST(value AS STRING)")

# from_json splits the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"U2..|
# +------------+
#
# and creates separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
structuredTopicFieldStreamingDF=kafkaStreamingDF\
    .withColumn("value", from_json("value", schema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("RedisSortedSet")

# this takes the element field from the 0th element in the array of structs and creates a column called encodedCustomer
zSetEncodedEntriesStreamingDF = spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet") 

zSetDecodedEntriesStreamingDF = zSetEncodedEntriesStreamingDF.withColumn("decodedCustomer", unbase64(zSetEncodedEntriesStreamingDF.encodedCustomer))

# this takes the stream and "materializes" or "executes" the flow of data and "sinks" it to the console
zSetDecodedEntriesStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
