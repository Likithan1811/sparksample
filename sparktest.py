# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySpark Sample") \
    .getOrCreate()

# Sample data (list of dictionaries)
data = [
    {"Name": "John", "Age": 28, "City": "New York"},
    {"Name": "Doe", "Age": 34, "City": "Los Angeles"},
    {"Name": "Alice", "Age": 25, "City": "Chicago"},
    {"Name": "Bob", "Age": 45, "City": "Houston"}
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create DataFrame from the list of dictionaries
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Print the schema of the DataFrame
df.printSchema()

# Filter rows where age is greater than 30
df_filtered = df.filter(df["Age"] > 30)

# Show the filtered DataFrame
df_filtered.show()

# Select specific columns (Name and City)
df_selected = df.select("Name", "City")

# Show the selected columns
df_selected.show()

# Stop the Spark session
spark.stop()
