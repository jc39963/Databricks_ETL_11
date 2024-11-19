from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import urllib.request




def extract_data(url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/nba-draft-2015/historical_projections.csv", download_path = "/dbfs/tmp/jdc_draft_2015.csv", file_path = "/tmp/jdc_draft_2015.csv"):
    spark = SparkSession.builder.appName("nbaDataPipeline").getOrCreate()
    # Download the file
    urllib.request.urlretrieve(url, download_path)
    print(f"File downloaded to {download_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform(df: DataFrame):
    conditions = [
        (F.col("Projected SPM") >= 0.5, "Yes"),
        (F.col("Projected SPM") < 0.5, "No")
    ]

    return df.withColumn("Projected Starter", F.when(conditions[0][0], conditions[0][1])
    .when(conditions[1][0], conditions[1][1])
    .otherwise("NA"))

def transform_load_data(df):
    # transforms data column names so it can be loaded in as a table
    df = df.select(
        [col(c).alias(c.replace(" ", "")) for c in df.columns]
    )
    df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("ids706_data_engineering.default.jdc_nba_2015")
    print("Data written to Databricks table.")




if __name__ == "__main__":
    df = extract_data()
    df = transform(df)
    df.show(5)
    transform_load_data(df)
    

