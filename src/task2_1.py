from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Airbnb Analysis").getOrCreate()

# Load the data file into a Spark DataFrame
df = spark.read.parquet("sf-airbnb-clean.parquet")

# Show the first 5 rows of the DataFrame
df.show(5)
