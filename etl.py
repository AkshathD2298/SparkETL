from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def main():
    spark = SparkSession.builder \
        .appName("CSV to Parquet ETL") \
        .getOrCreate()

    # Read CSV file
    df = spark.read.option("header", "true").csv("sales.csv")

    # Clean: drop rows with null price or quantity
    df_clean = df.dropna(subset=["price", "quantity"])

    # Cast price and quantity to correct types
    df_clean = df_clean.withColumn("price", col("price").cast("float")) \
                       .withColumn("quantity", col("quantity").cast("int"))

    # Convert order_date to date type
    df_clean = df_clean.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

    # Add a total_amount column
    df_clean = df_clean.withColumn("total_amount", col("price") * col("quantity"))

    # Write as Parquet partitioned by region
    df_clean.write.mode("overwrite").partitionBy("region").parquet("output/sales_parquet")

    spark.stop()

if __name__ == "__main__":
    main()
