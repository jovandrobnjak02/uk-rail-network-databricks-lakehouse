# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import boto3
import gzip

# COMMAND ----------

bucket = dbutils.widgets.get("s3_bucket")
object_prefix = dbutils.widgets.get("s3_object_prefix")
region = dbutils.widgets.get("region")

# COMMAND ----------

access_key = dbutils.secrets.get("darwin-s3-creds", "access_key_id")
secret_access_key = dbutils.secrets.get("darwin-s3-creds", "secret_access_key")

# COMMAND ----------

s3 = boto3.resource("s3", region_name = region, aws_access_key_id = access_key, aws_secret_access_key = secret_access_key)
bucket = s3.Bucket(bucket)

# COMMAND ----------

for obj in bucket.objects.limit(2):
    print(obj.key, obj.size, obj.last_modified, obj.e_tag)
    data = obj.get()['Body'].read()
    print(gzip.decompress(data).decode('utf-8'))