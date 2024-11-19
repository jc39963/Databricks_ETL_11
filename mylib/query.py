from pyspark.sql import SparkSession
def sql_query(spark:SparkSession):
    #df.createOrReplaceTempView("nba_data")
    result = spark.sql("""
        WITH avg_superstar AS (
        SELECT AVG(Superstar) AS avg_superstar_score
        FROM jdc_nba_2015
        )
        SELECT Player, Superstar - avg_superstar_score as Above_Average
        FROM jdc_nba_2015, avg_superstar
        WHERE Superstar > avg_superstar_score
        ORDER BY Superstar DESC;
                       """)
    
    result.show()


if __name__ == "__main__":
    session = SparkSession.builder.appName("nbaDataPipeline").getOrCreate()

    sql_query(session)
