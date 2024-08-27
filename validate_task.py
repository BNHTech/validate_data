#!/usr/bin/env python3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql.dataframe import DataFrame

HASH_COL_NAME: str = "ROW_HASH"
src: dict = {"user": "", "password": "", "table": "", "driver": "", "url": ""}
dest: dict = {"user": "", "password": "", "table": "", "driver": "", "url": ""}


def connect_to_spark():
    sparkConf = SparkConf()
    sparkConf.setMaster("spark://spark1.bnh.vn:7077")
    sparkConf.setAppName("KUBERNETES-IS-AWESOME")
    spark: SparkSession = (
        SparkSession.builder.config(conf=sparkConf)
        .config("spark.driver.host", "code-server.bnh.vn")
        .config("spark.driver.bindAddress", "code-server.bnh.vn")
        .config("spark.jars", "postgresql-42.7.3.jar")
        .getOrCreate()
    )
    sc: SparkContext = spark.sparkContext
    return spark, sc


spark, sc = connect_to_spark()


def get_table_pk_col(config: dict) -> str:
    """query database's information schema to get PK col name for target table"""
    query_str: str = ""
    d: str = config["driver"]
    output_col: str = "C_NAME"
    if d == "org.postgresql.Driver":
        query_str = f"""SELECT c.column_name as {output_col}
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = 'mytable';
"""
    elif d == "com.mysql.cj.jdbc.Driver":
        query_str = f"""SELECT k.column_name as {output_col}
FROM information_schema.table_constraints t
JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type='PRIMARY KEY'
  AND t.table_schema='YourDatabase'
  AND t.table_name='YourTable';
"""
    df = (
        spark.read.format("jdbc")
        .option("url", config["url"])
        .option("query", query_str)
        .option("user", config["user"])
        .option("password", config["password"])
        .load()
    )
    return df.select(output_col).collect()[0]


def get_original_datasets(
    table: str, pk: str, transformations: list[str]
) -> list[DataFrame]:
    """"""
    df_src: DataFrame = spark.read.jdbc(url=src["url"], table=table, properties=src)
    df_dest: DataFrame = spark.read.jdbc(url=dest["url"], table=table, properties=dest)
    for t in transformations:
        df_src: DataFrame = spark.sql("SELECT {t} FROM {df}", t=t, df=df_src)
        df_dest: DataFrame = spark.sql("SELECT {t} FROM {df}", t=t, df=df_dest)
    return [df_src, df_dest]


def get_dataset_hash(dfs: list[DataFrame], pk: str) -> list[DataFrame]:
    """"""

    df_src: DataFrame = spark.sql(
        "SELECT {pk}, {hash_col} FROM {df}",
        pk=pk,
        hash_col=HASH_COL_NAME,
        df=dfs[0].withColumn(HASH_COL_NAME, md5(concat_ws("||", *dfs[0].columns))),
    )
    df_dest: DataFrame = spark.sql(
        "SELECT {pk}, {hash_col} FROM {df}",
        pk=pk,
        hash_col=HASH_COL_NAME,
        df=dfs[1].withColumn(HASH_COL_NAME, md5(concat_ws("||", *dfs[1].columns))),
    )
    return [df_src, df_dest]


def compare_hashes(dfs: list[DataFrame], pk: str) -> DataFrame:
    """"""
    df: DataFrame = dfs[0].join(
        dfs[1].withColumnRenamed(HASH_COL_NAME, f"DEST_{HASH_COL_NAME}"),
        pk,
    )
    df = df.filter(df[HASH_COL_NAME] != df[f"DEST_{HASH_COL_NAME}"])
    return df


def get_invalid_rows(s_dfs: list[DataFrame], r_df: DataFrame, pk: str) -> DataFrame:
    """"""
    s_dfs[0] = s_dfs[0].join(r_df, pk)
    s_dfs[1] = s_dfs[1].join(r_df, pk)
    # rename dest columns
    for c in s_dfs[1].columns:
        if c.lower() == pk.lower():
            continue
        s_dfs[1] = s_dfs[1].withColumnRenamed(c, "DEST_" + c)
    return s_dfs[0].join(s_dfs[1], pk)
