#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param


df = spark.read.jdbc(
    "jdbc:postgresql://report.bnh.vn:5432/confluence",
    "content",
    properties={
        "user": "postgres",
        "password": "oracle_4U",
        "driver": "org.postgresql.Driver",
    },
)

df = df.withColumn("hash", md5(concat_ws("||", *df.columns)))
df2 = spark.sql("select hash from {src_df}", src_df=df).show()
