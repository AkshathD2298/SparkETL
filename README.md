# Spark CSV to Parquet ETL

## Setup

1. Install Python 3.7+ and Java 8+ (required by Spark).
2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

3. Run the ETL:

   ```
   python etl.py
   ```

4. Output Parquet files will be in `output/sales_parquet/`.

   When your ETL script runs, it writes Parquet data in this structure:

output/sales_parquet/
├── region=East/
│   └── part-*.parquet
├── region=North/
│   └── part-*.parquet
├── region=South/
│   └── part-*.parquet
└── region=West/
    └── part-*.parquet
Each region=XYZ folder contains Parquet data files for that region.

To read this output back in PySpark, you can do:
df = spark.read.parquet("output/sales_parquet")
df.show()



