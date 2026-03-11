from pyspark.sql import SparkSession
from pymongo import MongoClient
# Membuat SparkSession
spark = SparkSession.builder.getOrCreate()

# Membaca file CSV yang sudah ditransformasi dengan header dan inferSchema
df = spark.read.csv("/opt/airflow/data/Workflow_transformed",header=True,inferSchema=True)

# Mengonversi DataFrame Spark ke format yang bisa dimasukkan ke MongoDB
data = [row.asDict() for row in df.collect()]

# Koneksi ke MongoDBnya
client = MongoClient("mongodb+srv://blabla:YNTKTS@clusteeer0.hwedegz.mongodb.net/?retryWrites=true&w=majority")
# Akses database dan juga koleksi MongoDB

collection = client["P2M3_Database"]["workflow_data"]

# Menghapus data lama di koleksi MongoDB sebelum memasukkan data baru karena data ku cuma satu aja
collection.delete_many({})


#Memasukkan data ke MongoDB
collection.insert_many(data)

# Menutup koneksi ke MongoDB
client.close()

