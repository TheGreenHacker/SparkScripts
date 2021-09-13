#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler

ss = SparkSession.builder.appName("RealEstatePricePredictor").getOrCreate() 

data = ss.read.option("header", "true").option("inferSchema", "true").csv("realestate.csv")

# Transform data 
assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")
df = assembler.transform(data).select("PriceOfUnitArea", "features")

# Split data into training and test sets
(trainData, testData) = data.randomSplit([0.7, 0.3])

# Create decision tree regressor model
dt = DecisionTreeRegressor(featuresCol="features").setLabelCol("PriceOfUnitArea")

# Chain assembler and tree in pipeline
pipeline = Pipeline(stages=[assembler, dt])

# Train model
model = pipeline.fit(trainData)

# Make predictions
full_predictions = model.transform(testData)
predictions = full_predictions.select("prediction").rdd.map(lambda x: x[0])
prices = full_predictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

# Zip predictions and actual prices together
predictions_and_prices = predictions.zip(prices).collect()
for prediction in predictions_and_prices:
     print(prediction)

ss.stop()
