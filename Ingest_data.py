# Databricks notebook source
# Specify the URL of the dataset
import urllib.request

url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/nba-draft-2015/historical_projections.csv"
download_path = "/dbfs/tmp/jdc_draft_2015.csv"  # Path in DBFS

# Download the file
urllib.request.urlretrieve(url, download_path)

print(f"File downloaded to {download_path}")

# COMMAND ----------

file_path = "/tmp/jdc_draft_2015.csv"

# Load the file into Spark
df = spark.read.csv(file_path, header=True, inferSchema=True)
display(df)

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField


# Define variables used in the code below
file_path = "/dbfs/tmp/jdc_draft_2015.csv"
table_name = "jdc_draft_2015"
checkpoint_path = "/tmp/_checkpoint/jdc_draft_2015"

schema = StructType(
  [
    StructField("Player", StringType(), True),
    StructField("Position", StringType(), True),
    StructField("ID", StringType(), True),
    StructField("DraftYear", StringType(), True),
    StructField("ProjectedSPM", DoubleType(), True),
    StructField("Superstar", DoubleType(), True),
    StructField("Starter", DoubleType(), True),
    StructField("RolePlayer", IntegerType(), True),
    StructField("Bust", DoubleType(), True),
  ]
)

(spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "csv")
  .option("sep","\t")
  .load(file_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name)
)


# COMMAND ----------

df = spark.table("jdc_draft_2015")
df.show()
