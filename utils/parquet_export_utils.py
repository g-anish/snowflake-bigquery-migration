import re

from utils.constants import type_mapping


def clean_type(type_str):
    for pattern, replacement in type_mapping.items():
        if re.match(pattern, type_str):
            return replacement
    return type_str

def generate_snowflake_parquet_cast_selects(df):
    """Generates Snowflake SQL to cast using :: syntax for Parquet export.

    Args:
        df: DataFrame with columns 'column_name', 'data_type', 'cast_type'.

    Returns:
        SQL query string, or None if no columns.
    """
    if df.empty:
        return None

    select_list = []
    for _, row in df.iterrows():
        column_name = row['name']
        cast_type = row['cleaned_type']
        if cast_type:
            select_list.append(f"{column_name}::{cast_type} AS {column_name}")
        else:
            select_list.append(f"{column_name}")  # No casting if cast_type is not provided

    select_statement = ",\n        ".join(select_list)
    return select_statement

