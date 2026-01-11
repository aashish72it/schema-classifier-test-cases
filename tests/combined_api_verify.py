from pyschemaclassifier.verify import verify_schema, write_report

import inspect

print("write_report defined in:", inspect.getsourcefile(write_report))

def run_all():
    # 1) Compare schema files
    report = verify_schema("cli/schema_date.json", "cli/schema_delta.yml", pattern_mode="glob")
    print("=== Schema verification (dict) for 2 schema files ===")
    print(report["summary"], report["details"])

    # 2) Compare CSV files, output in text file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_20260101.csv",
    )
    write_report(report, fmt="txt", output_dir="./api/", output_file="verify_sales_2025_vs_2026.txt")

    # 3) Compare CSV files, output in json file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_20260101.csv",
    )
    write_report(report, fmt="json", output_dir="./api/", output_file="verify_sales_2025_vs_2026.json")


    # 4) Compare CSV files, output in yaml file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_20260101.csv",
    )
    write_report(report, fmt="yaml", output_dir="./api/", output_file="verify_sales_2025_vs_2026.yml")

    # 5) Compare CSV files, output in text file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_header.csv",
    )
    write_report(report, fmt="txt", output_dir="./api/", output_file="verify_sales_2025_vs_header.txt")

    # 6) Compare CSV files, output in json file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_header.csv",
    )
    write_report(report, fmt="json", output_dir="./api/", output_file="verify_sales_2025_vs_header.json")


    # 7) Compare CSV files, output in yaml file
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_header.csv",
    )
    write_report(report, fmt="yaml", output_dir="./api/", output_file="verify_sales_2025_vs_header.yml")

    # 8) Compare schema yaml files
    report = verify_schema(
        left="./cli/schema_orc.yml",
        right="./cli/schema_pqt.yml",
    )
    write_report(report, fmt="txt", output_dir="./api/", output_file="verify_cli_schema_2025_vs_2026.txt")

    # 9) Compare CSV files with optional arguments
    report = verify_schema(
        left="data/csv/sales_20250101.csv",
        right="data/csv/sales_20260101.csv",
        relaxed_types=True,        # allow int<->double; decimal<->double -> warning
        nullable_strict=False,     # warnings for nullable diffs (default)
        case_sensitive=True,       # default True; set False to ignore case in field names
        order_sensitive=False,     # default False; set True for sequence match
        pattern_mode=None,         # 'exact'|'glob'|'regex' if you want filename pattern checks
        dir_mode='first',          # for multi-file comparisons: 'first'|'any'|'all'
    )
    write_report(report, fmt="json", output_dir="./api/", output_file="verify_relaxed.json")

    # 10) Compare schema files
    report = verify_schema("cli/schema_never_existed.yml", "cli/schema_again_doesnt_exist.yml", pattern_mode="glob")
    print("=== For files which does not exist ===")
    print(report["summary"], report["details"])


if __name__ == "__main__":
    run_all()
