from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import requests
import os

def spark_session(appname:str) -> SparkSession:
    spark = SparkSession.builder.appName(appname).getOrCreate()
    return spark 

def extract_data(url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/nba-draft-2015/historical_projections.csv",
                 file_path = "data/nba_2015.csv",
                 directory = "data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def read_data(filepath, spark: SparkSession) -> DataFrame:
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    return df

def sql_query(df: DataFrame, spark:SparkSession):
    df.createOrReplaceTempView("nba_data")
    result = spark.sql("""
        WITH avg_superstar AS (
        SELECT AVG(Superstar) AS avg_superstar_score
        FROM nba_data
        )
        SELECT Player, Superstar - avg_superstar_score as Above_Average
        FROM nba_data, avg_superstar
        WHERE Superstar > avg_superstar_score
        ORDER BY Superstar DESC;
                       """)
    
    result.show()
    return result

def transform(df: DataFrame):
    conditions = [
        (F.col("Projected SPM") >= 0.5, "Yes"),
        (F.col("Projected SPM") < 0.5, "No")
    ]

    return df.withColumn("Projected Starter", F.when(conditions[0][0], conditions[0][1])
    .when(conditions[1][0], conditions[1][1])
    .otherwise("NA"))