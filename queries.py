from utils.parquet_export_utils import generate_snowflake_parquet_cast_selects

_TABLE_INFORMATION_QUERY = """
    SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';
            """

_TABLE_DESCRIPTION_QUERY = """
    DESCRIBE TABLE {table_schema}.{table_name};
    """

def get_table_information_query(table_schema: str,
                                table_name: str) -> str:
    return _TABLE_INFORMATION_QUERY.format(table_schema=table_schema,
                                  table_name=table_name)

def get_table_description_query(table_schema: str,
                                table_name: str) -> str:
    return _TABLE_DESCRIPTION_QUERY.format(table_schema=table_schema,
                                           table_name=table_name)

_SQL_QUERY_TEMPLATE = """
COPY INTO @ext_gcs_unload_stage 
FROM (
    SELECT
        {select_statement}
    FROM {table_schema}.{table_name}
)
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;
"""

def generate_sql_query(table_schema: str, table_name: str, df) -> str:
    select_statement = generate_snowflake_parquet_cast_selects(df)
    return _SQL_QUERY_TEMPLATE.format(select_statement=select_statement,
                                      table_schema=table_schema,
                                      table_name=table_name)


CREATE_EXT_STAGE_QUERY = """
CREATE OR REPLACE STAGE ext_gcs_unload_stage
  URL='gcs://snowflake-migration/users'
  STORAGE_INTEGRATION = GCS_INTEG_MIGRATION
  FILE_FORMAT = (TYPE = PARQUET); 
"""