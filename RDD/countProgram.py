# Example 6: Create a Base RDD and Transform it (Final Boss)

# Here are the brief steps for writing the word counting program:
# Create a base RDD from Complete_Shakespeare.txt file.
# Use RDD transformation to create a long list of words from each element of the base RDD.
# Remove stop words from your data.
# Create pair RDD where each element is a pair tuple of ('w', 1)
# Group the elements of the pair RDD by key (word) and add up their values.
# Swap the keys (word) and values (counts) so that keys is count and value is the word.
# Finally, sort the RDD by descending order and print the 10 most frequent words and their frequencies.


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FinalBoss") \
    .getOrCreate()

sc = spark.sparkContext

baseRDD = sc.textFile("Complete_Shakespeare.txt")
splitRDD = baseRDD.flatMap(lambda x: x.split())

print("Total number of words: ", splitRDD.count())

stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 
			  'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 
			  'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 
			  'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 
			  'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 
			  'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 
			  'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 
			  'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 
			  'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 
			  'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 
			  'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 
			  'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 
			  'now']
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# logic when we use stop_words as RDD 

# stop_words_rdd = sc.textFile("stop_words.txt").flatMap(lambda x: x.split())
# stop_words_set = set(stop_words_rdd.collect())
# stop_words_bc = sc.broadcast(stop_words_set)

# splitRDD_no_stop = splitRDD.filter(
#     lambda x: x.lower() not in stop_words_bc.value
# )

splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

print(resultRDD.collect())

for word in resultRDD.take(10):
	print(word)

resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

for word in resultRDD_swap_sort.take(10):
	print("{},{}".format(word[1], word[0]))
