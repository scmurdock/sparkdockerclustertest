from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split, lit, concat
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, IntegerType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
kafkaMessageSchema = StructType(
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

customerJSONSchema = StructType (
    [
        StructField("birthDay",StringType()),
        StructField("email",StringType())
    ]
)

# {"customer":"Bobby.Anandh@test.com","score":-3.0,"riskDate":"2020-09-09T17:05:34.350Z"} with timestamp: 1599692736490

customerRiskJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate",DateType())
    ]
)
# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("kafka-join-log").getOrCreate()
kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "Seans-MBP:9092") \
    .option("subscribe","redis-server")                  \
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
    .withColumn("value", from_json("value", kafkaMessageSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("RedisSortedSet")

# this executes a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and creates a column called encodedCustomer
# the reason we did it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
zSetEncodedEntriesStreamingDF = spark.sql("select key, zSetEntries[0].element as customer from RedisSortedSet") 

# unbase64() takes the redis kafka event string which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and converts it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"startTime":1599...|
#+--------------------+
zSetDecodedEntriesStreamingDF = zSetEncodedEntriesStreamingDF.withColumn("customer", unbase64(zSetEncodedEntriesStreamingDF.customer).cast("string"))

zSetDecodedEntriesStreamingDF\
    .withColumn("customer", from_json("customer", customerJSONSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("CustomerRecords")\

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null
emailAndBirthDayStreamingDF = spark.sql("select birthDay, email from CustomerRecords where birthday is not null and email is not null")

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email',split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# this takes the stream and "materializes" or "executes" the flow of data and "sinks" it to the console
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

logFolder = "/tmp/logs"  # Should be some file on your system

#logDataStreamingDF is a Streaming DataFrame - an unbounded table containing the streaming log data. One column of strings named "value". 
# Each log entry is a row in the "table" with the log folder as its source
# This streaming Dataframe isn't receiving data yet, because we haven't started it yet
logDataStreamingDF = spark.readStream.text(logFolder)

# this transformation (once the pipeline starts) will capture values containing this string, see example below:
# INFO: DeviceRouter received payload: {"customer":"Bobby.Anandh@test.com","score":-3.0,"riskDate":"2020-09-09T17:05:34.350Z"} with timestamp: 1599692736490

riskScoreStreamingDF = logDataStreamingDF.filter(logDataStreamingDF.value.contains('DeviceRouter received payload'))

# we are doing a "select" statement on the log entries with the json we want, and getting the json by asking for characters to the right of the {
# so we would get something that looks like this "customer":"sal.khan@test.com","score":-0.98,"riskDate":"2020-09-02T06:00:30.336Z"}
# what's missing? the leading {    
partialJsonStreamingDF = riskScoreStreamingDF.select(split(riskScoreStreamingDF.value,"\{").getItem(1).alias("json"))

# so we add that back in at the start to make the json complete
completeJsonStreamingDF=partialJsonStreamingDF.select(concat(lit("{"),partialJsonStreamingDF.json))

# parse the json string column into nested columns containing each field based on the schema provided, then save it as a temp view
completeJsonStreamingDF.withColumn("json", from_json("json", customerRiskJSONSchema)).select("json.*").createOrReplaceTempView("CustomerRisk")

# save the fields from the temporary view in a dataframe
riskScoreStreamingDF = spark.sql("select json.customer as customer, json.score as score, json.riskDate as riskDate from CustomerRisk")

# every transformation in the pipeline so far has a Streaming DataFrame as an input, and a Streaming DataFrame as an output
# this is different, as we want to sink to something outside, in this case the console
# so we are taking jsonFixed (a Streaming DataFrame) and getting a DataStreamWriter, in **Append** mode, to write to the Console
riskScoreStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()


