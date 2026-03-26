from pyspark.sql import SparkSession
import time
import psutil
import os

print("=" * 70)
print("EXPERIMENT 3: 3 DataNodes (Local emulation with 4 cores)")
print("=" * 70)

# Используем 4 ядра для эмуляции параллельной обработки на 3 узлах
spark = SparkSession.builder \
    .appName("Experiment3_3DN_Local") \
    .master("local[4]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

start_time = time.time()

print("\n1. Loading data from HDFS...")
df = spark.read.csv("hdfs://localhost:9000/user/data/ecommerce/ecommerce_dataset_150k_fixed.csv",
                    header=True, inferSchema=True, sep=',')

total = df.count()
print(f"   Loaded {total:,} records")
print(f"   Columns: {len(df.columns)}")
print(f"   Partitions: {df.rdd.getNumPartitions()}")

print("\n2. Running analysis...")

print("\n   Device type distribution:")
df.groupBy("device_type").count().show()

print("\n   Customer type distribution:")
df.groupBy("customer_type").count().show()

print("\n   Top 5 regions:")
df.groupBy("region").count().orderBy("count", ascending=False).show(5)

print("\n   Channel distribution:")
df.groupBy("channel").count().orderBy("count", ascending=False).show()

print("\n3. Numeric statistics:")
df.select(["month", "hour", "session_duration_sec", "pages_visited"]).describe().show()

end_time = time.time()
exec_time = end_time - start_time

process = psutil.Process(os.getpid())
mem = process.memory_info().rss / 1024 / 1024

print("\n" + "=" * 70)
print("EXPERIMENT 3 RESULTS (3 DataNodes Emulation)")
print("=" * 70)
print(f"Execution time: {exec_time:.2f} seconds")
print(f"Memory usage: {mem:.0f} MB")
print(f"Records processed: {total:,}")
print(f"Parallelism: local[4] (4 cores)")
print("=" * 70)

spark.stop()
print("\nExperiment 3 completed!")