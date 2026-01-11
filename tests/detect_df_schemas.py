#!/usr/bin/env python3

import json
import pandas as pd
#from rich import print_json
from pyspark.sql import SparkSession

from pyschemaclassifier.infer import detect_schema, write_schema


def detect_pandas_schema():
    # Sample pandas DataFrame
    pdf = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "price": [10.5, 20.0, 30.75],
        "active": [True, False, True],
        })

    schema = detect_schema(pandas_df=pdf)
    #print_json("Pandas schema", schema)
    return schema

def write_pandas_schema():
    schema = detect_pandas_schema()
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_pandas.yml")


def detect_spark_schema():
    spark = SparkSession.builder.master("local[1]").appName("Spark-Schema-Detect-Test").getOrCreate()

    try:
        # Sample Spark DataFrame
        data = [
            (1, "Alice", 10.5, True),
            (2, "Bob", 20.0, False),
            (3, "Charlie", 30.75, True),
        ]
        columns = ["id", "name", "price", "active"]
        sdf = spark.createDataFrame(data, schema=columns)

        schema = detect_schema(spark_df=sdf)
        #print_json("Spark schema", schema)
        return schema
    finally:
        spark.stop()

def write_spark_schema():
    schema = detect_spark_schema()
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_spark.yml")

def main():
    p_schema = detect_pandas_schema()
    s_schema = detect_spark_schema()

    if p_schema is None and s_schema is None:
        print("\nNo schemas detected because both pandas and PySpark are unavailable.")
    else:
        print("\nSchema detection completed.")

    print("\nWriting Pandas Schema...")
    write_pandas_schema()
    print("\nWriting Spark Schema...")
    write_spark_schema()

if __name__ == "__main__":
    main()
