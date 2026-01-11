# schema-classifier-test-cases
This repository is to test the use cases for schema-classifier python library


```bash

## To set up this repository, create and activate venv
python -m venv .venv
source .venv/bin/activate 
or 
.\.venv\Scripts\activate

python.exe -m pip install --upgrade pip
pip install -r requiremements.txt

```

```bash

## Execute the test scripts in following order to see the full potential. 

## First run the schema detect from CLI & API
python tests\combined_cli_detect.py
python tests\combined_api_detect_write.py

## Then run the schema verify from CLI & API to see the comparison between schema files generated via first 2 scripts
python tests\combined_cli_verify.py
python tests\combined_api_verify.py

## Following scripts can be executed independent of above commands as they showcase the schema detect & verify functionality for pandas & spark dataframe and you won't need files generated thru above scripts.

python tests\detect_df_schemas.py
python tests\verify_df_schemas.py

```