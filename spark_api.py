import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.types import IntegerType, StructField, LongType, BooleanType, DoubleType
from pyspark.sql.functions import from_json, explode

spark = SparkSession.builder.appName("weather_api").master("local[1]").getOrCreate()

output_path_california = "hdfs://localhost:9000/user/api_output/california/"
output_path_georgia = "hdfs://localhost:9000/user/api_output/georgia/"


# read the files from hdfs from California_states
df = spark \
    .read \
    .format("csv") \
    .option("delimiter", ",") \
    .load("hdfs://localhost:9000/user/api/california_cities_data.csv", header=False).toDF(
    "coord", "weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod")

temp_df = df.select("main", "name")

main_schema = StructType([
    StructField('temp', DoubleType(), True),
    StructField('feels_like', DoubleType(), True),
    StructField('temp_min', DoubleType(), True),
    StructField('temp_max', DoubleType(), True),
    StructField('pressure', IntegerType(), True),
    StructField('humidity', IntegerType(), True),
    StructField('sea_level', IntegerType(), True),
    StructField('grnd_level', IntegerType(), True),

])

temp_df = df.select(from_json(col("main"), main_schema).alias("main"), "name").withColumn("state", lit('california'))
# temp_df.withColumn("state",lit("california"))
split_df = temp_df.select("main.*", 'name', "state")

# split_df.show(truncate=False)

temp_df1 = split_df.withColumn("temp_celsius", (col("temp") - 32) * 5 / 9)
# temp_df1.show()
# group by city and calculate average and standard deviation of temperature
result_df_california = temp_df1.groupBy("state").agg(avg("temp_celsius").alias("avg_temp_celsius"),
                                                     stddev("temp_celsius").alias("stddev_temp_celsius"))

result_df_california.show()

## read the files from hdfs from Georgia_states
georgia_df = spark \
    .read \
    .format("csv") \
    .option("delimiter", ",") \
    .load("hdfs://localhost:9000/user/api/georgia_cities_data.csv", header=False).toDF(
    "coord", "weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod")

georgia_temp_df = georgia_df.select("main", "name")

main_schema = StructType([
    StructField('temp', DoubleType(), True),
    StructField('feels_like', DoubleType(), True),
    StructField('temp_min', DoubleType(), True),
    StructField('temp_max', DoubleType(), True),
    StructField('pressure', IntegerType(), True),
    StructField('humidity', IntegerType(), True),
    StructField('sea_level', IntegerType(), True),
    StructField('grnd_level', IntegerType(), True),

])

georgia_temp_df = georgia_df.select(from_json(col("main"), main_schema).alias("main"), "name").withColumn("state",
                                                                                                          lit('Georgia'))
split_df_georgia = georgia_temp_df.select("main.*", 'name', "state")

georgia_temp_df1 = split_df_georgia.withColumn("temp_celsius", (col("temp") - 32) * 5 / 9)
result_df_georgia = georgia_temp_df1.groupBy("state").agg(avg("temp_celsius").alias("avg_temp_celsius"),
                                                          stddev("temp_celsius").alias("stddev_temp_celsius"))

result_df_georgia.show()

result_df_georgia.write.csv(output_path_georgia, mode="overwrite", header=True)
result_df_california.write.csv(output_path_california, mode="overwrite", header=True)

print("stopping the SparkSession object")
spark.stop()
