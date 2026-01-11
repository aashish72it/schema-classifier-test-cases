import subprocess

subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_20260101.csv --fmt txt --output-dir ./cli --output-file verify_sales_2025_vs_2026.txt', shell=True, check=True)
subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_20260101.csv --fmt json --output-dir ./cli --output-file verify_sales_2025_vs_2026.json', shell=True, check=True)
subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_20260101.csv --fmt yaml --output-dir ./cli --output-file verify_sales_2025_vs_2026.yml', shell=True, check=True)
subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_header.csv --fmt json --output-dir ./cli --output-file verify_sales_2025_vs_header.json', shell=True, check=True)
subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_header.csv --fmt yaml --output-dir ./cli --output-file verify_sales_2025_vs_header.yml', shell=True, check=True)
subprocess.run('schema-verify data/csv/sales_20250101.csv data/csv/sales_header.csv --fmt txt --output cli/verify_sales_2025_vs_header.txt', shell=True, check=True)
subprocess.run('schema-verify ./cli/schema_never_existed.yml ./cli/schema_again_doesnt_exist.yml --fmt dict', shell=True, check=True)
subprocess.run('schema-verify ./api/schema_orc.yml ./api/schema_avro.yml --fmt json --output-dir ./cli --output-file verify_sales_orc_vs_avro.json', shell=True, check=True)
