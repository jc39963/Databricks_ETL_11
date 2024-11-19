from mylib.extract_transform_load import extract_data, transform, transform_load_data
from mylib.query import sql_query
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col

if __name__ == "__main__":
    session = SparkSession.builder.appName("nbaDataPipeline").getOrCreate()
    df = extract_data()
    df = transform(df)
    print("Dataframe with transformation extra column: Projected Starter:")
    df.show()
    transform_load_data(df)
    print("Result of query:")
    sql_query(session)