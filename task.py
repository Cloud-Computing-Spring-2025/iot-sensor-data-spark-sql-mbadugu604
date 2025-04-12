from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

# Load CSV
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create Temp View
df.createOrReplaceTempView("sensor_readings")

# Basic Queries
df.show(5)
print("Total Records:", df.count())
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

# Save output
df.write.csv("task1_output.csv", header=True)

# Task 2

# Filtering temperature
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
out_of_range = df.filter((df.temperature < 18) | (df.temperature > 30))
print("In-range:", in_range.count())
print("Out-of-range:", out_of_range.count())

# Aggregation
agg_df = df.groupBy("location").agg(
    {"temperature": "avg", "humidity": "avg"}
).withColumnRenamed("avg(temperature)", "avg_temperature")\
 .withColumnRenamed("avg(humidity)", "avg_humidity")\
 .orderBy("avg_temperature", ascending=False)

agg_df.show()

# Save output
agg_df.write.csv("task2_output.csv", header=True)

# Task 3

from pyspark.sql.functions import hour, to_timestamp

# Convert timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df.createOrReplaceTempView("sensor_readings")

# Group by hour
hourly_avg = df.withColumn("hour_of_day", hour("timestamp"))\
    .groupBy("hour_of_day")\
    .agg({"temperature": "avg"})\
    .withColumnRenamed("avg(temperature)", "avg_temp")\
    .orderBy("hour_of_day")

hourly_avg.show()

# Save output
hourly_avg.write.csv("task3_output.csv", header=True)


# Task 4

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, dense_rank

# Avg temp by sensor
sensor_avg = df.groupBy("sensor_id")\
    .agg(avg("temperature").alias("avg_temp"))

# Window rank
window_spec = Window.orderBy(sensor_avg["avg_temp"].desc())
ranked = sensor_avg.withColumn("rank_temp", dense_rank().over(window_spec))

# Top 5 sensors
top5 = ranked.orderBy("rank_temp").limit(5)
top5.show()

# Save output
top5.write.csv("task4_output.csv", header=True)

# Task 5

from pyspark.sql.functions import hour

# Add hour column
df = df.withColumn("hour_of_day", hour("timestamp"))

# Pivot
pivot_df = df.groupBy("location")\
    .pivot("hour_of_day", list(range(24)))\
    .avg("temperature")

pivot_df.show(truncate=False)

# Save output
pivot_df.write.csv("task5_output.csv", header=True)