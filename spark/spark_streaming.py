from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType

# Hydrometric data Schema
schema = StructType([
    StructField("code_site", StringType(), True),
    StructField("code_station", StringType(), True),
    StructField("grandeur_hydro", StringType(), True),
    StructField("date_debut_serie", StringType(), True),
    StructField("date_fin_serie", StringType(), True),
    StructField("statut_serie", IntegerType(), True),
    StructField("code_systeme_alti_serie", IntegerType(), True),
    StructField("date_obs", StringType(), True),
    StructField("resultat_obs", FloatType(), True),
    StructField("code_methode_obs", IntegerType(), True),
    StructField("libelle_methode_obs", StringType(), True),
    StructField("code_qualification_obs", IntegerType(), True),
    StructField("libelle_qualification_obs", StringType(), True),
    StructField("continuite_obs_hydro", BooleanType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True)
])

# Create a Spark Session
spark = SparkSession.builder \
    .appName("HydrometrieKafkaStreaming") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hydrometrie_topic") \
    .load()

# Apply JSON schema and extract fields
df_json = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema ).alias("data")) \
            .select("data.*")

# Writing data in PostgreSQL
df_json.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write  \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "hydrometrie_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver","org.postgresql.Driver")
        .mode("append") \
        .save())  \
    .outputMode("append")  \
    .start() \
    .awaitTermination()