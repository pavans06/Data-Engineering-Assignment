from pyspark import SparkContext

# a)

# Load the data file into an RDD
rdd = sc.textFile("groceries.csv")

# Split each transaction into individual items
items = rdd.flatMap(lambda line: line.split(','))

# Get the unique items
unique_items = items.distinct()

# Save the unique items to a text file
unique_items.saveAsTextFile("out/out_1_2a.txt")


# To save the output into a single file
unique_items.coalesce(1).saveAsTextFile("out/out_1_2a.txt")

# b)

# Split each transaction into individual items
items = rdd.flatMap(lambda line: line.split(','))

# Get the count of items
count = items.count()

# Save the count to a text file
with open("out/out_1_2b.txt", "w") as f:
    f.write("Count: {}\n".format(count))





