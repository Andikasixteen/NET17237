from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('WordCount').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
# input data
data = sc.textFile("/input/dataWordCount.txt")
# process
counts = data.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# output
counts.saveAsTextFile("/ResultWordCountPySpark")
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))
