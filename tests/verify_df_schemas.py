#!/usr/bin/env python3
import json

from pyschemaclassifier.infer import detect_schema
from pyschemaclassifier.verify import verify_schema
try:
    import pandas as pd
except Exception:
    pd = None

SparkSession = None
try:
    from pyspark.sql import SparkSession
except Exception as e:
    print(f"\n[PySpark] Import failed: {e}\nSkipping Spark verification.")

def print_json(title: str, obj: dict):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, indent=2))

def build_pandas_df():
    if pd is None:
        return None
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "price": [10, 20, 30],  # int
    })

def build_spark_df(spark):
    data = [
        (1, "Alice", 10.5, True),
        (2, "Bob", 20.0, False),
        (3, "Charlie", 30.75, True),
    ]
    columns = ["id", "name", "price", "active"]
    return spark.createDataFrame(data, schema=columns)

def detect_schema_pandas(pdf):
    return detect_schema(pandas_df=pdf)

def detect_schema_spark(sdf):
    return detect_schema(spark_df=sdf)

def verify_two_schemas(left_schema: dict, right_schema: dict):
    return verify_schema(left=left_schema, right=right_schema)

def main():
    p_schema = None
    if pd is not None:
        pdf = build_pandas_df()
        p_schema = detect_schema_pandas(pdf)
        print_json("Detected pandas schema", p_schema)
    else:
        print("\n[pandas] Not installed; skipping pandas detection.")

    s_schema = None
    if SparkSession is not None:
        spark = SparkSession.builder.master("local[1]").appName("SchemaVerifyTest").getOrCreate()
        try:
            sdf = build_spark_df(spark)
            s_schema = detect_schema_spark(sdf)
            print_json("Detected Spark schema", s_schema)
        finally:
            spark.stop()
    else:
        print("\n[PySpark] Not available; skipping Spark detection.")

    if p_schema and s_schema:
        report = verify_two_schemas(left_schema=p_schema, right_schema=s_schema)
        print_json("Verification report (pandas vs spark)", report)

    else:
        print("\nNot enough schemas to run cross verification. Need both pandas and Spark.")

if __name__ == "__main__":
    main()
