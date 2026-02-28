from pyspark.sql import SparkSession
from ingestion.bronze import create_bronze_tables
from transformation.silver import create_silver_table
from transformation.gold import create_gold_transactions

def run():

    spark = SparkSession.builder.appName("Europe3Pipeline").getOrCreate()

    create_bronze_tables(spark)
    create_silver_table(spark)
    create_gold_transactions(spark)

    print("Pipeline execution completed.")

if __name__ == "__main__":
    run()