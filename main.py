import os
from typing import Optional

import click
import pandas as pd

from helpers.bigquery_helper import BigQueryHelperGoogleCloud, CloudStorageHelperGoogleCloud
from helpers.snowflake_helper import fetch_all as snowflake_fetch_all
from queries import get_table_description_query, generate_sql_query, CREATE_EXT_STAGE_QUERY

from utils.parquet_export_utils import clean_type
from utils.time_utils import timeit

pd.options.display.max_columns = 50
pd.options.display.width = 5000

@timeit
def get_table_information(table_schema: str,
                          table_name: str,
                          database: str) -> pd.DataFrame:
    query: str = get_table_description_query(table_schema=table_schema,
                                             table_name=table_name)

    data: list[Optional] = snowflake_fetch_all(query=query)
    df = pd.DataFrame(data)
    print(f'Table Information df shape -  {df.shape}')
    df['cleaned_type'] = df['type'].apply(clean_type)
    return df[['name', 'type', 'cleaned_type']]


def create_ext_stage(table_schema: str):
    query: str = CREATE_EXT_STAGE_QUERY
    data: list[Optional] = snowflake_fetch_all(query=query, schema=table_schema)
    print(f'Table Information shape - {data}')


@timeit
def load_data_to_gcs_stage(query: str, table_schema: str):
    data: list[Optional] = snowflake_fetch_all(query=query, schema=table_schema)
    print(f'Table Information shape - {data}')


@click.command()
@click.option('--table_schema', '-m', required=False, type=str, help='Table schema')
@click.option('--table_name', '-m', required=False, type=str, help='Table Name')
@click.option('--database', '-m', required=False, type=str, help='Database Name')
def main(table_schema: str, table_name: str, database: str):
    if not table_schema:
        table_schema = click.prompt('Please enter the table schema', type=str)
    if not table_name:
        table_name = click.prompt('Please enter the table name', type=str)
    if not database:
        database = click.prompt('Please enter the database name', type=str)

    # 1. Get Table Information
    table_info_df = get_table_information(table_schema=table_schema,
                                          table_name=table_name,
                                          database=database)
    print(table_info_df)

    # Create or Replace stage
    create_ext_stage(table_schema=table_schema)

    # 2. COPY INTO @your_stage_name
    copy_into_sql_query = generate_sql_query(table_schema=table_schema,
                                             table_name=table_name,
                                             df=table_info_df)
    print(copy_into_sql_query)
    load_data_to_gcs_stage(copy_into_sql_query, table_schema=table_schema)

    # GCS to BigQuery load job
    # Initialize BigQueryHelper

    bq_helper = BigQueryHelperGoogleCloud(
        credentials_path='<>/snowflake_data_migration/service_account_credential.json',
        project_id='<project_id>'
    )

    ##Truncate table if needed
    bq_helper.truncate_table(dataset_id='<dataset_id>', table_id='<table_id>')

    # Load data from GCS to BigQuery
    bq_helper.load_data_from_gcs_to_bigquery(
        gcs_bucket_name='snowflake-migration',
        gcs_file_name='users',
        dataset_id='<dataset_id>',
        table_id='table_id'
    )

    # Initialize BigQueryHelper
    cloud_storage_helper = CloudStorageHelperGoogleCloud(
        credentials_path='<>/snowflake_data_migration/service_account_credential.json',
        project_id='<project_id>'
    )

    cloud_storage_helper.delete_storge_folder(bucket_name='snowflake-migration', folder_name='users')


if __name__ == '__main__':
    main()
