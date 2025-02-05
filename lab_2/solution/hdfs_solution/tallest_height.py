from pyspark import SparkContext
import os

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

# Initialize Spark context
sc = SparkContext("spark://0783b2bd22d4:7077", "tallest_height")

# Read the dataset
rdd = sc.textFile("hdfs://0783b2bd22d4:9000/input/arbres.csv")

# Map each line to a tuple of (height, genus), and handle invalid height values
height_genus_pairs = rdd.map(lambda line: (line.split(";")[7], line.split(";")[3])) \
    .filter(lambda pair: pair[0].replace('.', '', 1).isdigit())  # Ensure height is a valid number

# Convert height to float and sort by height in descending order
height_genus_pairs = height_genus_pairs.map(lambda pair: (float(pair[0]), pair[1]))

# Sort by height in descending order and get the tallest tree
tallest_tree = height_genus_pairs.sortByKey(False).first()

# Display the genus of the tallest tree
print("tallest tree is: ", {tallest_tree[1]})

