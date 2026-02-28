from pyspark.sql import SparkSession

def create_bronze_tables(spark: SparkSession):

    # Assuming raw data already uploaded to volume
    contracts = spark.read.option("header", "true") \
        .csv("/Volumes/workspace/default/raw/contracts/")

    claims = spark.read.option("header", "true") \
        .csv("/Volumes/workspace/default/raw/claims/")

    contracts.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable("workspace.default.contracts")

    claims.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable("workspace.default.claims")

    print("Bronze tables created successfully.")