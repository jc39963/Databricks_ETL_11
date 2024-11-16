"""
Main cli or app entry point
"""
from mylib.lib import spark_session, extract_data, read_data, sql_query, transform

if __name__ == "__main__":
    session = spark_session("PySpark")
    extract_data()
    data = read_data("data/nba_2015.csv", session)
    print("Result of query:")
    sql_query(data, session)
    print("Data before transformation:")
    print(data.show())
    transformed_data = transform(data)
    print("Data after tranformation:")
    print(transformed_data.show())
