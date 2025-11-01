from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder.appName("KafkaSparkStreaming_Ciudades").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir esquema del JSON (ciudad, temperatura, humedad, timestamp)
schema = StructType([
    StructField("city", StringType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# Leer los datos del topic de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear el JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular promedios por ciudad en ventanas de 1 minuto
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), col("city")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Mostrar resultados en consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
