from pyspark.sql.functions import col

# filter for properties with lowest price and highest rating
lowest_price_highest_rating = df.filter((col("price") == df.selectExpr("MIN(price)").collect()[0][0]) & 
                                         (col("review_scores_rating") == 100))

# get the number of people that can be accommodated by this property
num_people = lowest_price_highest_rating.select("accommodates").collect()[0][0]

# write the number of people to a text file
with open("out/out_2_4.txt", "w") as f:
    f.write(str(num_people))
