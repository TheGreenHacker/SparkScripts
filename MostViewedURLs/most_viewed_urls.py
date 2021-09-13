#!/usr/local/bin/python3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import regexp_extract, window, current_timestamp, col

DIRECTORY = "logs"

ss = SparkSession.builder.appName("MostViewedURLs").getOrCreate()

# Turn off logging at INFO level
ss.sparkContext.setLogLevel('WARN')

lines = ss.readStream.text(DIRECTORY)

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

df = lines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep track of most-viewed URL's in logs by 30 second windows, sampled every 10 seconds
window_df = df.withColumn("event_time", current_timestamp())
window_df_counts = window_df.groupBy(window(col("event_time"), "30 seconds", "10 seconds"), col("endpoint")).count()
sorted_window_df_counts = window_df_counts.orderBy(col("count").desc())

# Kick off our streaming query, dumping results to the console
query = (sorted_window_df_counts.writeStream.outputMode("complete").format("console").queryName("counts").start())

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
ss.stop()

