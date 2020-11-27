from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

customerRiskJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate",DateType())
    ]
)


# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("stedi-events").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "Seans-MBP:9092") \
    .option("subscribe","stedi-events")                  \
    .option("startingOffsets","earliest")\
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
kafkaStreamingDF\
    .withColumn("value", from_json("value", customerRiskJSONSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("CustomerRisk")

# this executes a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and creates a column called encodedCustomer
# the reason we did it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk") 


# this takes the stream and "materializes" or "executes" the flow of data and "sinks" it to the console as it is updated one at a time like this:
# +--------------------+-----+
# |            customer|score|
# +--------------------+-----+
# |Spencer.Davis@tes...|  8.0|
# +--------------------+-----+
customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

