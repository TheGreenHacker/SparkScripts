#!/usr/local/bin/python3 
from pyspark.sql import SparkSession
from pyspark.sql import Row

def parseLine(line):
    fields = line.split(',')
    return Row(id=int(fields[0]), amount=float(fields[2]))

ss = SparkSession.builder.appName("CustomerSpendings").getOrCreate()

lines = ss.sparkContext.textFile("customer-orders.csv")
customers = lines.map(parseLine)

schemaCustomers = ss.createDataFrame(customers).cache()
schemaCustomers.createOrReplaceTempView("customers")

spendings = ss.sql("SELECT id, ROUND(SUM(amount), 2) AS total_amount FROM customers GROUP BY id ORDER BY total_amount")
spendings.show(spendings.count())

ss.stop()