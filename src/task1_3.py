from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local", "Grocery Shopping Analysis")

# Load the data file into an RDD
rdd = sc.textFile("groceries.csv")

# Split each transaction into individual items
items = rdd.flatMap(lambda line: line.split(','))

# Map each item to a tuple of (item, 1) for counting
item_tuples = items.map(lambda item: (item, 1))

# Reduce by key to count the frequency of each item
item_counts = item_tuples.reduceByKey(lambda a, b: a + b)

# Sort by frequency in descending order
top_items = item_counts.sortBy(lambda x: x[1], False)

# Take the top 5 items
top_5_items = top_items.take(5)

# Save the results to a text file
with open("out/out_1_3.txt", "w") as f:
    for item in top_5_items:
        f.write("{}\n".format(item))
