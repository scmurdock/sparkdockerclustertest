from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType

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

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("stedi-kafka").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

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


# this takes the stream and "materializes" or "executes" the flow of data and "sinks" it to the console as it is updated one at a time like this:
#+--------------------+---------+
#|               email|birthYear|
#+--------------------+---------+
#|Gail.Spencer@test...|     1963|
#|Craig.Lincoln@tes...|     1962|
#|  Edward.Wu@test.com|     1961|
#|Santosh.Phillips@...|     1960|
#|Sarah.Lincoln@tes...|     1959|
#|Sean.Howard@test.com|     1958|
#|Sarah.Clark@test.com|     1957|
#|Suresh.Clark@test...|     1956|
#|  Lyn.Davis@test.com|     1955|
#|  Trevor.Wu@test.com|     1954|
#|Edward.Khatib@tes...|     1953|
#|Bobby.Jackson@tes...|     1951|
#|David.Gonzalez@te...|     1949|
#|Danny.Smith@test.com|     1948|
#|     Lyn.Wu@test.com|     1946|
#|Travis.Jones@test...|     1945|
#|Suresh.Olson@test...|     1944|
#|Ashley.Mitra@test...|     1943|
##|Angie.Aristotle@t...|     1940|
#| John.Mitra@test.com|     1939|
#+--------------------+---------+
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

#zSetDecodedEntriesStreamingDF\
#    .selectExpr("CAST(customer AS STRING)")\
#    .writeStream\
#    .format("kafka")\
#    .option("kafka.bootstrap.servers", "Seans-MBP:9092") \
#    .option("topic", "test-output")\
#    .option("checkpointLocation","/tmp/checkpoint")\
#    .start()\
#    .awaitTermination()
