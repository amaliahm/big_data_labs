from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, sum, countDistinct

spark = SparkSession.builder.appName("lab3").getOrCreate()

df = spark.read.csv("hdfs://0783b2bd22d4:9000/input/ngram.csv", header=False, inferSchema=True, sep='\t')
df = df.toDF('bigram','year','count', 'pages','books')

print('schema')
df.printSchema()

print("####################")

print('10 rows')
df.show(10)

print("####################")

# tmp table
df.createOrReplaceTempView("ngram")

print('3.1) Return all bigrams where the Count is greater than five')

print('sql')
result = spark.sql("Select * from ngram where count > 5").show(10)
print(result)

print('sparksql')
result = df.filter("count > 5").select("*").show(10)
print(result)


print("####################")


print('3.2) Return the total number of bigrams for each year.')

print('sql')
result = spark.sql('select year, count(*) as total_bigram from ngram group by year').show(10)
print(result)

print('sparksql')
result = df.groupBy('year').agg(count('*').alias('total_bigram')).show(10)
print(result)


print("####################")


print('3.3) Return the bigrams with the highest Count for each year.')

print('sql')
result = spark.sql('select year, bigram, MAX(count) as max_count from ngram group by year, bigram').show(10)
print(result)

print('sparksql')
result = df.groupBy("year","bigram").max("count").show(10)
print(result)


print("####################")


print('3.4) Return all bigrams that appeared in 20 different years.')

print('sql')
result = spark.sql('select bigram from ngram group by bigram having count(distinct year) = 20').show(10)
print(result)

print('sparksql')
result = df.groupby('bigram').agg(countDistinct('year').alias('distinct_year')).filter('distinct_year = 20').show(10)
print(result)


print("####################")
 

print('3.5) Return all bigrams that contain the character \'!\' in the first part and the character \'9\' in the second part (the two parts are separated by a space).')

print('sql')
result = spark.sql('select year, bigram from ngram where bigram like "%!% %9%"').show(10)
print(result)

print('sparksql')
result = df.filter('bigram like "%%! %9%"').select('year', 'bigram').show(10)
print(result)


print("####################")


print('3.6) Return the bigrams that appeared in all the years present in the dataset.')

print('sql')
result = spark.sql('select bigram from ngram group by bigram having count(distinct year) = (select count(distinct year) from ngram)').show(10)
print(result)

print('sparksql')
result = df.groupby('bigram').agg(countDistinct('year').alias('distinct_year')).filter(col('distinct_year') == df.select(countDistinct('year')).collect()[0][0]).show(10)
print(result)


print("####################")


print('3.7) Return the total number of pages and books in which each bigram appeared for each available year, sorted in alphabetical order.')

print('sql')
result = spark.sql('select year, bigram, sum(pages) as total_pages, sum(books) as total_books from ngram group by year, bigram order by bigram').show(10)
print(result)

print('sparksql')
result = df.groupby('year', 'bigram').agg(sum('pages').alias('total_pages'), sum('books').alias('total_books')).orderBy('bigram').show(10)
print(result)


print("####################")


print('3.8) Return the total number of distinct bigrams for each year, sorted in descending order of the year.')

print('sql')
result = spark.sql('select year, count(distinct bigram) as distinct_bigram from ngram group by year order by year desc').show(10)
print(result)

print('sparksql')
result = df.groupBy('year').agg(countDistinct('bigram').alias('distinct_bigram')).orderBy('year', ascending=False).show(10)
print(result)
