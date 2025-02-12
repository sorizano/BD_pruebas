from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, broadcast

# Configuración optimizada
spark = SparkSession.builder \
    .appName("EDA_BigData_Tuning") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Cargar datos desde HDFS
file_path = "hdfs://localhost:9000/user/datasets/synthetic_sales_data.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Revisar número de particiones
print(f"Número de particiones antes de optimizar: {data.rdd.getNumPartitions()}")

# Reducir particiones si es necesario
data = data.repartition(20)
print(f"Número de particiones después de optimizar: {data.rdd.getNumPartitions()}")


# Caché si se usará varias veces
data.cache()

# Persistir en memoria y disco si es muy grande
data.persist()


# Crear un DataFrame pequeño para pruebas
small_df = data.select("Region").distinct()

# Hacer un Join optimizado
optimized_join = data.join(broadcast(small_df), "Region")
optimized_join.show(5)
