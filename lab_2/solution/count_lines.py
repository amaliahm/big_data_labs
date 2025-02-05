from pyspark import SparkConf, SparkContext
import os

print('start')

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
sc = SparkContext("spark://0783b2bd22d4:7077", "lab2")

rdd = sc.textFile("file:///root/arbres.csv")

nbr = rdd.count()

print("nbr of lines: ",nbr)

sc.stop()

