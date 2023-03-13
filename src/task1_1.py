import urllib.request

# Download the data from the URL and save it to a local file
url = 'https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv'
filename = 'groceries.csv'
urllib.request.urlretrieve(url, filename)

from pyspark import SparkContext

# Load the data file into an RDD
rdd = sc.textFile("groceries.csv")

# Display the first 5 rows of the RDD
print(rdd.take(5))


