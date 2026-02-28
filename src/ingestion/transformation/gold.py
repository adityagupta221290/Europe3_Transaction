from pyspark.sql.functions import *
from delta.tables import DeltaTable

def create_gold_transactions(spark):

    silver = spark.table("workspace.default.europe3_silver")

    gold_df = (
        silver
        .withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3"))
        .withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("CONTRACT_ID").cast("long"))
        .withColumn("SOURCE_SYSTEM_ID",
            regexp_replace("CLAIM_ID", "^[A-Z_]+", "").cast("int"))
        .withColumn("TRANSACTION_TYPE",
            when(col("CLAIM_TYPE") == "2", "Corporate")
            .when(col("CLAIM_TYPE") == "1", "Private")
            .otherwise("Unknown"))
        .withColumn("TRANSACTION_DIRECTION",
            when(col("CLAIM_ID").startswith("CL"), "COINSURANCE")
            .when(col("CLAIM_ID").startswith("RX"), "REINSURANCE"))
        .withColumn("CONFORMED_VALUE", col("AMOUNT").cast("decimal(16,5)"))
        .withColumn("BUSINESS_DATE", to_date("DATE_OF_LOSS", "dd.MM.yyyy"))
        .withColumn("CREATION_DATE",
            to_timestamp("CREATION_DATE", "dd.MM.yyyy HH:mm"))
        .withColumn("SYSTEM_TIMESTAMP", current_timestamp())
        .withColumn("NSE_ID", md5(col("CLAIM_ID")))
    )

    table_name = "workspace.default.transactions"

    if not spark.catalog.tableExists(table_name):
        gold_df.write.format("delta") \
            .saveAsTable(table_name)
        print("Gold table created.")
    else:
        delta = DeltaTable.forName(spark, table_name)
        delta.alias("t").merge(
            gold_df.alias("s"),
            "t.NSE_ID = s.NSE_ID"
        ).whenNotMatchedInsertAll().execute()
        print("Gold table merged successfully.")