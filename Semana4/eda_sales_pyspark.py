from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Iniciar Spark
spark = SparkSession.builder.appName("EDA_BigData").getOrCreate()

# Ruta en HDFS
file_path = "hdfs://localhost:9000/user/datasets/synthetic_sales_data.csv"

try:
    # Intentar cargar datos desde HDFS
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    print("Archivo CSV cargado exitosamente desde HDFS.")
    
    # Mostrar primeros datos
    data.show(5)

    # Resumen estadístico
    data.describe().show()

    # Conteo de valores nulos
    data.select([count(when(col(c).isNull(), c)).alias(c) for c in data.columns]).show()

except Exception as e:
    print("\nError al cargar archivo desde HDFS:\n", str(e).encode("utf-8", "ignore").decode("utf-8"))
