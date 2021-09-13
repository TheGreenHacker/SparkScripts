#!/usr/local/bin/python3 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpendings")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    id = int(fields[0])
    price = float(fields[2])
    return (id, price)

lines = sc.textFile("customer-orders.csv")
spendings_by_customer = lines.map(parseLine).reduceByKey(lambda x, y: x + y)
spendings_by_customer_sorted = spendings_by_customer.map(lambda x: (x[1], x[0])).sortByKey()

results = spendings_by_customer_sorted.collect()
for result in results:
    print(result)
