# Snowflake to BigQuery Migration boiler plate code  

## Local Setup Run Instructions
Setup virtualenv and install packages listed in requirements.txt. 
Run below commands from the project root directory.

```text
pip install -r requirements.txt
```

### Pre-requisites
1. Create a service account in GCP and download the service account key file.
2. External storage integration 'GCS_INTEG_MIGRATION' should be created in snowflake, and corresponding service account should have the GCS bucket permissions. (See manual steps in the blog) 
3. Set the Snowflake credentials in the environment variables. (Check more on **snowflake_helper** file)

## Run Instructions
```text
python main.py --table_schema=TABLE_SCHEMA_VALUE --table_name=TABLE_NAME_VALUE 
```

## Developer notes
* All SQL queries are located in `queries.py` python module.
