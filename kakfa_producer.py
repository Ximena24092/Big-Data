# ===========================================
# Procesamiento batch con Spark (EDA de ciudades)
# ===========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, desc

# 1️⃣ Crear la sesión de Spark
spark = SparkSession.builder.appName("EDA_Ciudades_Batch").getOrCreate()

# 2️⃣ Cargar el conjunto de datos desde archivo CSV
# Reemplaza la ruta si lo guardaste en otra carpeta
df = spark.read.csv("file:///home/vboxuser/ciudades.csv", header=True, inferSchema=True)

print("=== Datos originales cargados ===")
df.show(5)

# 3️⃣ Limpieza de datos
# Eliminar registros nulos o duplicados
df_clean = df.na.drop().dropDuplicates()

print("=== Después de limpiar datos nulos o duplicados ===")
df_clean.show(5)

# 4️⃣ Transformaciones
# Convertir la columna 'date' a tipo fecha
df_clean = df_clean.withColumn("date", col("date").cast("date"))

# Crear una nueva columna: rango térmico (clasificar temperaturas)
df_transformed = df_clean.withColumn(
    "rango_temp",
    col("temperature")
        .when(col("temperature") < 20, "Frío")
        .when((col("temperature") >= 20) & (col("temperature") <= 28), "Templado")
        .otherwise("Cálido")
)

print("=== Después de las transformaciones ===")
df_transformed.show(5)

# 5️⃣ Análisis exploratorio (EDA)
print("=== Promedio de temperatura y humedad por ciudad ===")
df_transformed.groupBy("city").agg(
    avg("temperature").alias("avg_temp"),
    avg("humidity").alias("avg_hum")
).show()

print("=== Temperatura máxima y mínima registradas ===")
df_transformed.select(
    max("temperature").alias("max_temp"),
    min("temperature").alias("min_temp")
).show()

print("=== Cantidad de registros por rango térmico ===")
df_transformed.groupBy("rango_temp").agg(count("*").alias("cantidad")).orderBy(desc("cantidad")).show()

# 6️⃣ Guardar resultados procesados
df_transformed.write.mode("overwrite").csv("file:///home/vboxuser/resultados_eda_ciudades")

spark.stop()
