#!/usr/local/bin/python3 
from pyspark.sql import SparkSession
from pyspark.sql import Row

def parseGraph(line):
	fields = line.split(' ')
	return Row(id=int(fields[0]), connections=len(fields) - 1)

def parseNames(line):
    fields = line.split(' ', 1)
    return Row(id=int(fields[0]), name=str(fields[1]))

ss = SparkSession.builder.appName("ObscureSuperheros").getOrCreate()
graphLines = ss.sparkContext.textFile("Marvel-Graph.txt")
nameLines = ss.sparkContext.textFile("Marvel-Names.txt")
graph = graphLines.map(parseGraph)
names = nameLines.map(parseNames)

schemaGraph = ss.createDataFrame(graph).cache()
schemaGraph.createOrReplaceTempView("graph")
schemaNames = ss.createDataFrame(names).cache()
schemaNames.createOrReplaceTempView("names")

graph = ss.sql("SELECT id, SUM(connections) AS total_connections FROM graph GROUP BY id")
graph.createOrReplaceTempView("graph")
obscureHeros = ss.sql("SELECT name FROM names JOIN graph ON names.id = graph.id WHERE total_connections IN (SELECT MIN(total_connections) FROM graph)")
obscureHeros.show(obscureHeros.count(), False)   

ss.stop()