from pyspark.sql.functions import col

def create_silver_table(spark):

    contracts = spark.table("workspace.default.contracts")
    claims = spark.table("workspace.default.claims")

    # Fix typo column
    claims = claims.withColumnRenamed("CONTRAT_ID", "CONTRACT_ID")

    silver = claims.join(
        contracts,
        (claims.CONTRACT_ID == contracts.CONTRACT_ID) &
        (claims.CONTRACT_SOURCE_SYSTEM == contracts.SOURCE_SYSTEM),
        "inner"
    )

    silver.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable("workspace.default.europe3_silver")

    print("Silver table created successfully.")