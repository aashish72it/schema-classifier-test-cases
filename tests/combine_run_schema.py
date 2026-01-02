from pyschemaclassifier import detect_schema, write_schema

def run_all():
    # 1) CSV with header
    schema = detect_schema(input_path="data/csv/sales_header.csv", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema.yml")

    # 2) CSV without header -> write to schema_no_header.yml
    schema = detect_schema(input_path="data/csv/sales_no_header.csv", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_no_header.yml")

    # 3) Very wide CSV -> write to schema_wide.yml
    schema = detect_schema(input_path="data/csv/very_wide.csv", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_wide.yml")

    # 4) CSV with UTF-8 BOM -> write to schema_utf8.yml
    schema = detect_schema(input_path="data/csv/sales_utf8_sig.csv", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_utf8.yml")

    # 5) ORC -> write to schema_orc.yml
    schema = detect_schema(input_path="data/orc/TestOrcFile.testDate1900.orc", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_orc.yml")

    # 6) AVRO -> write to schema_avro.yml
    schema = detect_schema(input_path="data/avro/weather.avro", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_avro.yml")

    # 7) Parquet -> write to schema_pqt.yml
    schema = detect_schema(input_path="data/parquet/v0.7.1.all-named-index.parquet", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_pqt.yml")

    # 8) Delta table directory -> write to schema_delta.yml
    schema = detect_schema(input_path="data/delta/people_countries_delta_dask/", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_delta.yml")

    # 9) NDJSON -> write to schema_json.yml
    schema = detect_schema(input_path="data/json/events.ndjson", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_json.yml")

    # 10) XML -> write to schema_xml.yml
    schema = detect_schema(input_path="data/xml/books.xml", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="yaml", output_file="schema_xml.yml")
    
    # 11) CSV folder -> write to {file}.schema.yml
    schema = detect_schema(input_path="data/csv/", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", multi_file_fmt="txt")

    # 12) CSV -> write to schema.json
    schema = detect_schema(input_path="data/csv/sales_20250101.csv", detection_mode="auto_detect")
    write_schema(schema, out_dir="./api/", fmt="json", output_file="schema_date.json")

    # 13) NDJSON with fmt=dict -> print to stdout (no file write)
    schema = detect_schema(input_path="data/json/events.ndjson", detection_mode="auto_detect")
    # Emulate `--fmt dict`: print raw schema structure to console
    print("=== Schema (dict) for data/json/events.ndjson ===")
    print(schema)


if __name__ == "__main__":
    run_all()
