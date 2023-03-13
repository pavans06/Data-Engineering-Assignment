from pyspark.sql.functions import avg

# Filter the DataFrame based on the given conditions
filtered_df = df.filter((df.price > 5000) & (df.review_scores_value == 10))

# Calculate the average number of bathrooms and bedrooms
avg_bathrooms = filtered_df.select(avg("bathrooms")).first()[0]
avg_bedrooms = filtered_df.select(avg("bedrooms")).first()[0]

# Create a new DataFrame with the calculated values
result_df = spark.createDataFrame([(avg_bathrooms, avg_bedrooms)], ["avg_bathrooms", "avg_bedrooms"])

# Write the DataFrame to a CSV file
result_df.write.csv("out/out_2_3.txt", mode="overwrite", header=True)

# Print the result DataFrame
result_df.show()
