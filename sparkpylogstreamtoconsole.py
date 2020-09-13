# Some comments here are based on the open source guide to Spark Structured Programming here: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, concat, lit, from_json
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, IntegerType, DateType

logFolder = "/tmp/logs"  # Should be some folder on your system

# {"customer":"Bobby.Anandh@test.com","score":-3.0,"riskDate":"2020-09-09T17:05:34.350Z"} with timestamp: 1599692736490

customerRiskJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate",DateType())
    ]
)

# this creates the spark application
spark = SparkSession.builder.appName("stedi-logging").getOrCreate()

# we don't want the large amount of spurrious log entries that come by default
spark.sparkContext.setLogLevel("ERROR")

#logDataStreamingDF is a Streaming DataFrame - an unbounded table containing the streaming log data. One column of strings named "value". 
# Each log entry is a row in the "table" with the log folder as its source
# This streaming Dataframe isn't receiving data yet, because we haven't started it yet
logDataStreamingDF = spark.readStream.text(logFolder)

# this transformation (once the pipeline starts) will capture values containing this string, see example below:
# INFO: Risk for customer: Ashley.Aristotle@test.com {"customer":"Ashley.Aristotle@test.com","score":4.5,"riskDate":"2020-09-12T22:37:39.626Z"}

riskScoreStreamingDF = logDataStreamingDF.filter(logDataStreamingDF.value.contains('INFO: Risk for customer: '))

# we are doing a "select" statement on the log entries with the json we want, and getting the json by asking for characters to the right of the {
# so we would get something that looks like this "customer":"sal.khan@test.com","score":-0.98,"riskDate":"2020-09-02T06:00:30.336Z"}
# what's missing? the leading {    
partialJsonStreamingDF = riskScoreStreamingDF.select(split(riskScoreStreamingDF.value,"\{").getItem(1).alias("json"))

# so we add that back in at the start to make the json complete
completeJsonStreamingDF=partialJsonStreamingDF.select(concat(lit("{"),partialJsonStreamingDF.json).alias("json"))

# parse the json string column into nested columns containing each field based on the schema provided, then save it as a temp view
completeJsonStreamingDF.withColumn("json", from_json("json", customerRiskJSONSchema)).select("json.*").createOrReplaceTempView("CustomerRisk")

# save the fields from the temporary view in a dataframe
riskScoreStreamingDF = spark.sql("select customer, score, riskDate from CustomerRisk")

# every transformation in the pipeline so far has a Streaming DataFrame as an input, and a Streaming DataFrame as an output
# this is different, as we want to sink to something outside, in this case the console
# so we are taking jsonFixed (a Streaming DataFrame) and getting a DataStreamWriter, in **Append** mode, to write to the Console
logDataStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
