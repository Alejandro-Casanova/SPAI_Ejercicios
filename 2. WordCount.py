import findspark
from pyspark import SparkContext
import re

findspark.init()
sc = SparkContext("local[*]", "Name of the Program")

# Load Text File
rdd1 = sc.textFile("quijote.txt")

# Remove all punctuation (just keep letters and spaces)
rdd2 = rdd1.map(lambda x: re.sub(r'[^A-Za-zÁÉÍÓÚáéíóú\s]', '', x))

# Split string into words and return tuple (word,1)
rdd3 = rdd2.flatMap(lambda x: [(word,1) for word in x.split()])

# Add up pairs with the same key
rdd4 = rdd3.reduceByKey(lambda x, y : x + y)

# Find the most used word
rdd5 = rdd4.reduce(lambda x, y: x if x[1] > y[1] else y)

print ("result: ", rdd5)