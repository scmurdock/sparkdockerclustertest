# Some comments here are based on the open source guide to Spark Structured Programming here: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit

logFolder = "/tmp/logs"  # Should be some file on your system
spark = SparkSession.builder.appName("stedi-logging").getOrCreate()


#logDataStreamingDF is a Streaming DataFrame - an unbounded table containing the streaming log data. One column of strings named "value". 
# Each log entry is a row in the "table" with the log folder as its source
# This streaming Dataframe isn't receiving data yet, because we haven't started it yet
logDataStreamingdf = spark.readStream.text(logFolder)

# this transformation (once the pipeline starts) will capture values containing this string, see example below:
# INFO: DeviceRouter received payload: {"customer":"Bobby.Anandh@test.com","score":-3.0,"riskDate":"2020-09-09T17:05:34.350Z"} with timestamp: 1599692736490

riskScoreStreamingdf = logDataStreamingdf.filter(logDataStreamingdf.value.contains('DeviceRouter received payload'))

# we are doing a "select" statement on the log entries with the json we want, and getting the json by asking for characters to the right of the {
# so we would get something that looks like this "customer":"sal.khan@test.com","score":-0.98,"riskDate":"2020-09-02T06:00:30.336Z"}
# what's missing? the leading {    
json = riskScoreStreamingdf.select(split(riskScoreStreamingdf.value,"\{").getItem(1).alias("json"))

# so we add that back in at the start to make the json complete
jsonFixed=json.select(concat(lit("{"),json.json))

# every transformation in the pipeline so far has a Streaming DataFrame as an input, and a Streaming DataFrame as an output
# this is different, as we want to sink to something outside, in this case the console
# so we are taking jsonFixed (a Streaming DataFrame) and getting a DataStreamWriter, in **Append** mode, to write to the Console
jsonFixed.writeStream.outputMode("append").format("console").start().awaitTermination()

