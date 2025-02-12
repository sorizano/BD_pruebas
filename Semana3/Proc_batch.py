# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# Iniciar Spark
spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

# Leer archivo CSV
file_path = "sales_data.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Analizar ventas por región
sales_by_region = data.groupBy("Region").sum("Sales")
sales_by_region.show()
