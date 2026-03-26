from pyspark.sql import SparkSession
import time
import psutil
import os

print("=" * 70)
print("EXPERIMENT 4: 3 DataNodes WITH OPTIMIZATIONS (Local emulation)")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Experiment4_3DN_Local_Opt") \
    .master("local[4]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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
print(f"   Initial partitions: {df.rdd.getNumPartitions()}")

print("\n2. Applying optimizations...")
print("   Repartitioning to 16 partitions...")
df = df.repartition(16)

print("   Caching data...")
df.cache()
df.count()
print("   Cache applied")

print("\n3. Running analysis...")

print("\n   Device type distribution:")
df.groupBy("device_type").count().show()

print("\n   Customer type distribution:")
df.groupBy("customer_type").count().show()

print("\n   Top 5 regions:")
df.groupBy("region").count().orderBy("count", ascending=False).show(5)

print("\n   Channel distribution:")
df.groupBy("channel").count().orderBy("count", ascending=False).show()

print("\n4. Numeric statistics:")
df.select(["month", "hour", "session_duration_sec", "pages_visited"]).describe().show()

end_time = time.time()
exec_time = end_time - start_time

process = psutil.Process(os.getpid())
mem = process.memory_info().rss / 1024 / 1024

print("\n" + "=" * 70)
print("EXPERIMENT 4 RESULTS (3 DataNodes + Optimizations)")
print("=" * 70)
print(f"Execution time: {exec_time:.2f} seconds")
print(f"Memory usage: {mem:.0f} MB")
print(f"Records processed: {total:,}")
print(f"Parallelism: local[4] (4 cores)")
print(f"Optimizations: repartition(16), cache, adaptive execution")
print("=" * 70)

spark.stop()
print("\nExperiment 4 completed!")