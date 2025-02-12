from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, stddev, mean, min, max

# Iniciar Spark
spark = SparkSession.builder.appName("Data_Validation_BigData").getOrCreate()

# Ruta en HDFS
file_path = "hdfs://localhost:9000/user/datasets/synthetic_sales_data.csv"

try:
    # Intentar cargar datos desde HDFS
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    print("Archivo CSV cargado exitosamente desde HDFS.\n")

    # Mostrar primeros datos
    data.show(5)

    # Validación 1: Verificar valores nulos
    print("\nVerificando valores nulos por columna:")
    data.select([count(when(col(c).isNull(), c)).alias(c) for c in data.columns]).show()

    # Validación 2: Verificar duplicados
    print("\nCantidad de filas duplicadas:")
    duplicates = data.count() - data.dropDuplicates().count()
    print(f"Filas duplicadas: {duplicates}")

    # Validación 3: Verificar tipos de datos
    print("\nEsquema del DataFrame:")
    data.printSchema()

    # Validación 4: Estadísticas generales (para detectar valores anómalos)
    print("\nEstadísticas generales de las columnas numéricas:")
    numeric_cols = [c[0] for c in data.dtypes if c[1] in ['int', 'double', 'float']]
    if numeric_cols:
        data.select([mean(c).alias(f"{c}_mean") for c in numeric_cols]).show()
        data.select([stddev(c).alias(f"{c}_stddev") for c in numeric_cols]).show()
        data.select([min(c).alias(f"{c}_min") for c in numeric_cols]).show()
        data.select([max(c).alias(f"{c}_max") for c in numeric_cols]).show()
    else:
        print("No hay columnas numéricas en los datos.")

except Exception as e:
    print("\nError al cargar y validar datos desde HDFS:\n", str(e).encode("utf-8", "ignore").decode("utf-8"))
