from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()
#membaca file CSV yang sudah diextract dengan header dan inferSchema
file_path = "/opt/airflow/data/AI_Workflow_Optimization_Dataset_2500_Rows_v1.csv"

#Membuat DataFrame dengan membaca file CSV, dengan opsi header dan inferSchema
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)
#Menambahkan kolom baru "Delay_Status" berdasarkan nilai "Delay_Flag"
df_transformed = df.withColumn(
    "Delay_Status",
    when(col("Delay_Flag") == 1, "Delay").otherwise("No Delay")
)

# Simpan ke CSV (1 file) untuk dipakai load
df_transformed.coalesce(1).write.mode("overwrite").csv("/opt/airflow/data/Workflow_transformed",header=True)