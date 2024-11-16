"""
Test goes here

"""
import os
from pyspark.sql import SparkSession
from mylib.lib import extract_data, spark_session, read_data, sql_query, transform


def test_spark():
    spark = spark_session("Pyspark")
    assert isinstance(spark, SparkSession), "Test failed."
    print("Spark session passed successfully.")


def test_extract_data():
    result = extract_data()
    assert os.path.exists(result)
    print("successfully extracted data")


def test_read_data():
    session = spark_session("PySpark")
    data = read_data("data/nba_2015.csv", session)
    assert data.columns == [
        "Player",
        "Position",
        "ID",
        "Draft Year",
        "Projected SPM",
        "Superstar",
        "Starter",
        "Role Player",
        "Bust",
    ]
    print("successfully read data")


def test_sql_query():
    session = spark_session("PySpark")
    data = read_data("data/nba_2015.csv", session)
    result = sql_query(data, session)
    assert result.columns == ["Player", "Above_Average"]
    print("successful query")


def test_transform():
    session = spark_session("PySpark")
    data = read_data("data/nba_2015.csv", session)
    result = transform(data)
    assert result.columns == [
        "Player",
        "Position",
        "ID",
        "Draft Year",
        "Projected SPM",
        "Superstar",
        "Starter",
        "Role Player",
        "Bust",
        "Projected Starter",
    ]
    print("successful transformation")


if __name__ == "__main__":
    test_spark()
    test_extract_data()
    test_read_data()
    test_sql_query()
    test_transform()
