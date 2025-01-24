import os
from contextlib import closing
from typing import Optional

import snowflake.connector


def get_snowflake_connection(user: str,
                             password: str,
                             account: str,
                             database: str,
                             warehouse: Optional[str] = None,
                             schema: Optional[str] = None) -> snowflake.connector.connection:
    conn = snowflake.connector.connect(user=user,
                                       password=password,
                                       account=account,
                                       warehouse=warehouse,
                                       database=database,
                                       role= "ROLE"
                                       )
    if schema:
        conn.cursor().execute(f"USE SCHEMA {schema}")
    return conn


def fetch_all(query: str, schema: Optional[str] = None) -> list[Optional]:
    snowflake_conn = get_snowflake_connection(user=os.getenv('snowflake_user'),
                                              password=os.getenv('snowflake_password'),
                                              account=os.getenv('snowflake_account'),
                                              database=os.getenv('snowflake_database'),
                                              warehouse=os.getenv('snowflake_warehouse'),
                                              schema=schema)
    with closing(snowflake_conn) as connection:
        with closing(connection.cursor()) as cur:
            print(f'Query: {query}')
            cur.execute(query)
            col_names = [i[0].lower() for i in cur.description]
            return [dict(zip(col_names, row)) for row in cur]
