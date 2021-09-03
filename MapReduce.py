#Reference http://spark.apache.org/examples.html

import pyspark

file = sc.textFile("*.json")
tweet_data = json.loads(file)

FM = text_file.flatMap(lambda line: line.split(" ")) 
MAP = FM.map(lambda word: (word, 1))
count = MAP.reduceByKey(lambda a, b: a + b)

counts.coalesce(1).saveAsTextFile("WordCount.txt")