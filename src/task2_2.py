from pyspark.sql.functions import count, min, max

# Calculate the minimum price, maximum price, and row count
min_price = df.select(min("price")).first()[0]
max_price = df.select(max("price")).first()[0]
row_count = df.count()

# Create a new DataFrame with the calculated values
result_df = spark.createDataFrame([(min_price, max_price, row_count)], ["min_price", "max_price", "row_count"])

# Write the DataFrame to a CSV file
result_df.write.csv("out/out_2_2.txt", mode="overwrite", header=True)

# Print the result DataFrame
result_df.show()
