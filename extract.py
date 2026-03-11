from pyspark.sql import SparkSession
# Membuat SparkSession
spark = SparkSession.builder.getOrCreate()
# Mendefinisikan path file CSV yang akan diextract
file_path = "/opt/airflow/data/AI_Workflow_Optimization_Dataset_2500_Rows_v1.csv"

#Ngebaca file CSV dengan header dan inferSchema
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

#soalnya cuma diminta buat baca saja


