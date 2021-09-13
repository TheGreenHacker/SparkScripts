#!/usr/local/bin/python3 
from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("FriendsByAge").master("local[*]").getOrCreate() 
people = ss.read.option("header", "true").option("inferSchema", "true")\
    .csv("friends-header.csv")
people.createOrReplaceTempView("people")

friends_by_age = ss.sql("SELECT age, ROUND(SUM(friends) / COUNT(age), 2) AS avg_friends FROM people GROUP BY age ORDER BY age")
friends_by_age.show(friends_by_age.count())

ss.stop()