from pyspark.sql import SparkSession

logFile = "/tmp/logs/stedi.log"  # Should be some file on your system
spark = SparkSession.builder.appName("stedi-logging").getOrCreate()
logDataDF = spark.read.text(logFile).cache()

riskScoreDF = logDataDF.filter(logDataDF.value.contains('Risk for customer'))

print("Lines with Risk Score: %i" % (riskScoreDF.count()))

riskScoreDF.printSchema()

#for riskScoreString in riskScoreStrings:
#    print(riskScoreString)

spark.stop()