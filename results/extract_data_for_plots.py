from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

print("=" * 70)
print("EXTRACTING DATA FROM HDFS FOR VISUALIZATION")
print("=" * 70)

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("DataExtractor") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("\n1. Loading data from HDFS...")
df = spark.read.csv("hdfs://localhost:9000/user/data/ecommerce/ecommerce_dataset_150k_fixed.csv",
                    header=True, inferSchema=True, sep=',')

total = df.count()
print(f"   Loaded {total:,} records")

# =============================================
# Сбор данных для графиков
# =============================================

print("\n2. Collecting region data...")
regions_df = df.groupBy("region").count().orderBy("count", ascending=False)
regions_pd = regions_df.toPandas()
print(f"   Found {len(regions_pd)} regions")

print("\n3. Collecting device type data...")
devices_df = df.groupBy("device_type").count().orderBy("count", ascending=False)
devices_pd = devices_df.toPandas()
print(f"   Found {len(devices_pd)} device types")

print("\n4. Collecting customer type data...")
customers_df = df.groupBy("customer_type").count().orderBy("count", ascending=False)
customers_pd = customers_df.toPandas()
print(f"   Found {len(customers_pd)} customer types")

print("\n5. Collecting channel data...")
channels_df = df.groupBy("channel").count().orderBy("count", ascending=False)
channels_pd = channels_df.toPandas()
print(f"   Found {len(channels_pd)} channels")

print("\n6. Collecting numeric statistics...")
numeric_stats = df.select(["month", "hour", "session_duration_sec", "pages_visited"]).describe().toPandas()

# Сохраняем данные в CSV для дальнейшего использования
regions_pd.to_csv("regions_data.csv", index=False, encoding='utf-8')
devices_pd.to_csv("devices_data.csv", index=False, encoding='utf-8')
customers_pd.to_csv("customers_data.csv", index=False, encoding='utf-8')
channels_pd.to_csv("channels_data.csv", index=False, encoding='utf-8')
numeric_stats.to_csv("numeric_stats.csv", index=False, encoding='utf-8')

print("\n[OK] Data saved to CSV files:")
print("   - regions_data.csv")
print("   - devices_data.csv")
print("   - customers_data.csv")
print("   - channels_data.csv")
print("   - numeric_stats.csv")

# =============================================
# Вывод данных в консоль
# =============================================
print("\n" + "=" * 70)
print("REGIONS DISTRIBUTION:")
print("=" * 70)
for _, row in regions_pd.iterrows():
    print(f"   {row['region']:<25} {row['count']:>8,} transactions")

print("\n" + "=" * 70)
print("DEVICE TYPE DISTRIBUTION:")
print("=" * 70)
for _, row in devices_pd.iterrows():
    pct = row['count'] / total * 100
    print(f"   {row['device_type']:<15} {row['count']:>8,} transactions ({pct:.1f}%)")

print("\n" + "=" * 70)
print("CUSTOMER TYPE DISTRIBUTION:")
print("=" * 70)
for _, row in customers_pd.iterrows():
    pct = row['count'] / total * 100
    print(f"   {row['customer_type']:<15} {row['count']:>8,} transactions ({pct:.1f}%)")

print("\n" + "=" * 70)
print("CHANNEL DISTRIBUTION:")
print("=" * 70)
for _, row in channels_pd.iterrows():
    pct = row['count'] / total * 100
    print(f"   {row['channel']:<15} {row['count']:>8,} transactions ({pct:.1f}%)")

print("\n" + "=" * 70)
print("NUMERIC STATISTICS:")
print("=" * 70)
print(numeric_stats.to_string())

spark.stop()
print("\n[OK] Data extraction completed!")