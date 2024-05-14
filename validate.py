#!/usr/bin/env python3

import sys
import pandas as pd
import oracledb
from sqlalchemy import create_engine, text as sql_text
from pandas.util import hash_pandas_object

dest_connection_str = "mysql+pymysql://TEST_GO:oracle_4U@192.168.100.35/test"
src_connection_str = "oracle://TEST_GO:oracle_4U@10.10.11.58:1521/tafjr22"
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
conn_src = create_engine(src_connection_str)
conn_dest = create_engine(dest_connection_str)


table_name: str = sys.argv[1]
print_output: str = len(sys.argv) > 2
if "." not in table_name:
    print("input does not follow format {SCHEMA}.{TABLE_NAME}")
    sys.exit(1)


def compare_row() -> bool:
    query = f"""SELECT '{table_name}' as tbl, count(1) as c FROM {table_name}"""
    src = pd.read_sql_query(con=conn_src.connect(), sql=sql_text(query))
    dest = pd.read_sql_query(con=conn_dest.connect(), sql=sql_text(query))
    df = src.merge(dest, how="inner", on="tbl", suffixes=["_src", "_dest"])
    df["comp"] = df["c_src"] == df["c_dest"]
    print("rows comparison")
    print(df.to_markdown())
    return df["comp"][0]


def get_key_col(table_name: str):
    ddl = table_name.split(".")
    schema = ddl[0]
    table = ddl[1]
    query = f"""SELECT k.column_name
FROM information_schema.table_constraints t
    JOIN information_schema.key_column_usage k USING(constraint_name, table_schema, table_name)
WHERE t.constraint_type = 'PRIMARY KEY'
    AND t.table_schema = '{schema}'
    AND t.table_name = '{table}'"""
    print(
        f"""---------
getting KEY columns: {query}"""
    )
    key_cols = pd.read_sql_query(con=conn_dest.connect(), sql=sql_text(query))
    return key_cols["column_name"]


def get_src():
    query = f"""SELECT * FROM (SELECT * FROM  {table_name}  ORDER BY dbms_random.value ) WHERE rownum <100"""
    print(
        f"""---------
query source: {query}"""
    )
    df = pd.read_sql_query(con=conn_src.connect(), sql=sql_text(query))
    return df


def get_target(k: str, key_list: list[any]):
    if isinstance(key_list[0], str):
        key_list = [f"'{key}'" for key in key_list]
        filter_cond = f"""CONVERT({k}, char) IN ( { ",".join(key_list) } )"""
    else:
        key_list = [str(key) for key in key_list]
        filter_cond = f"""CONVERT({k}, char) IN ({",".join(key_list)})"""
    query = f"""SELECT * FROM {table_name} WHERE {filter_cond}"""
    print(
        f"""---------
query target: {query}"""
    )
    df = pd.read_sql_query(con=conn_dest.connect(), sql=sql_text(query))
    return df


compare_row_success: bool = compare_row()

if not compare_row_success:
    print("comparison failed!!")
    sys.exit(1)

key_cols = get_key_col(table_name=table_name)
df_src = get_src()
df_src.columns = map(str.lower, df_src.columns)
df_src = df_src.set_index(key_cols[0].lower())
df_dest = get_target(key_cols[0], df_src.index.values.tolist())
df_dest.columns = map(str.lower, df_dest.columns)
df_dest = df_dest.set_index(key_cols[0].lower())

if print_output:
    print("source...")
    print(df_src.to_markdown())
    print("")
    print("dest....")
    print(df_dest.to_markdown())

# hashing rows
df_src["hash_src"] = hash_pandas_object(df_src)
df_dest["hash_dest"] = hash_pandas_object(df_dest)
# df_dest["hash_dest"] = df_dest.apply(lambda x: hash(tuple(x)), axis=1)

df_src = df_src[["hash_src"]]
df_dest = df_dest[["hash_dest"]]

df = df_src.join(df_dest)
df["comp"] = df["hash_dest"] == df["hash_src"]
print("---------")
print("data comparison")
print(df.to_markdown())

print("---------")

if False in df["comp"].tolist():
    print("comparison failed!!")
