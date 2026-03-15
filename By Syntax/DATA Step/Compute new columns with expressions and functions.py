"""
COMPUTE NEW COLUMNS WITH EXPRESSIONS AND FUNCTIONS (PySpark)

    This program uses PySpark to create a new DataFrame
        with selected rows and columns, new computed columns,
        and formatted values.
    Keywords: PySpark, calculated columns
    Original SAS Version: DATA STEP
    Documentation:
        https://spark.apache.org/docs/latest/api/python/
        reference/pyspark.sql/index.html

    1. Read the existing table sashelp.cars
       (loaded from CSV or existing source).
    2. Filter rows where Origin equals "Europe".
    3. Create new columns KmH_Highway and KmH_City that
       multiply MPG_Highway and MPG_City by 1.609344.
    4. Create a new column KmH_Average that calculates
       the mean of KmH_Highway and KmH_City.
    5. Round KmH columns to one decimal place
       (equivalent to SAS format 4.1).
    6. Keep only the columns: Make, Model, Type, and
       all columns starting with KmH.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("Compute New Columns")
    .getOrCreate()
)

# 1. Read the existing table
#    Replace the path with your cars dataset location.
cars = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sashelp_cars.csv")
)

# 2. Filter rows where Origin equals "Europe"
cars_europe = cars.filter(col("Origin") == "Europe")

# 3. Create new columns KmH_Highway and KmH_City
cars_new = (
    cars_europe
    .withColumn(
        "KmH_Highway", col("MPG_Highway") * 1.609344
    )
    .withColumn(
        "KmH_City", col("MPG_City") * 1.609344
    )
)

# 4. Create KmH_Average (mean of KmH_Highway and KmH_City)
cars_new = cars_new.withColumn(
    "KmH_Average",
    (col("KmH_Highway") + col("KmH_City")) / 2
)

# 5. Round KmH columns to one decimal place
#    (equivalent to SAS format 4.1)
cars_new = (
    cars_new
    .withColumn(
        "KmH_Highway",
        spark_round(col("KmH_Highway"), 1)
    )
    .withColumn(
        "KmH_City",
        spark_round(col("KmH_City"), 1)
    )
    .withColumn(
        "KmH_Average",
        spark_round(col("KmH_Average"), 1)
    )
)

# 6. Keep only selected columns
cars_new = cars_new.select(
    "Make", "Model", "Type",
    "KmH_Highway", "KmH_City", "KmH_Average"
)

# Display first 10 rows (equivalent to proc print obs=10)
print("First 10 Rows of CARS_NEW")
cars_new.show(10)

# Stop Spark session
spark.stop()
