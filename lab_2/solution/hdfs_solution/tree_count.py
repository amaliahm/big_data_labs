from pyspark import SparkContext
import os

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

# Initialize Spark context
sc = SparkContext("spark://0783b2bd22d4:7077", "tree_count")

# Read the dataset
rdd = sc.textFile("hdfs://0783b2bd22d4:9000/input/arbres.csv")

# Map each line to a tuple of (genus, 1) to count each occurrence of a genus
genus_counts = rdd.map(lambda line: (line.split(";")[3], 1))

# Use reduceByKey to aggregate the counts by genus
genus_counts_aggregated = genus_counts.reduceByKey(lambda x, y: x + y)

# Collect the results and display them
results = genus_counts_aggregated.collect()

# Print the number of trees for each genus
for genus, count in results:
    print("nbr of trees genus ", genus," is: ",count)

