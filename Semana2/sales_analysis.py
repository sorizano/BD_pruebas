from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Cargar el archivo CSV desde HDFS
file_path = "hdfs://localhost:9000/user/yourusername/sales_data.csv"
sales_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Mostrar los datos cargados
print("Datos cargados:")
sales_data.show()

# Calcular las ventas totales por región
sales_by_region = sales_data.groupBy("Region").agg(_sum("Sales").alias("Total_Sales"))
print("Ventas totales por región:")
sales_by_region.show()

# Determinar el producto más vendido en cada región
top_product_per_region = sales_data.groupBy("Region", "Product").agg(_sum("Sales").alias("Total_Sales")) \
    .withColumn("Rank", _max("Total_Sales").over(Window.partitionBy("Region").orderBy(col("Total_Sales").desc()))) \
    .filter(col("Rank") == 1) \
    .select("Region", "Product", "Total_Sales")

print("Producto más vendido por región:")
top_product_per_region.show()

# Guardar los resultados en HDFS
sales_by_region.write.csv("hdfs://localhost:9000/user/yourusername/sales_by_region", header=True)
top_product_per_region.write.csv("hdfs://localhost:9000/user/yourusername/top_product_per_region", header=True)

# Cerrar la sesión de Spark
spark.stop()
