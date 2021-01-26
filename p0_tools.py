
import os
import sys
from string import punctuation
spark_path = r"/home/clint/spark-3.0.1-bin-hadoop2.7" # spark installed folder
os.environ['SPARK_HOME'] = spark_path
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/'
sys.path.insert(0, spark_path + "/bin")
sys.path.insert(0, spark_path + "/python/pyspark/")
sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9-src.zip")
from operator import *

def length(x):
	if len(x) > 1:
		return x

def punct(x):
    punctuations = (",", ".", "!", "?", "'", ":", ";")
    if x[0] in punctuations:
        x = x[1:]
    elif x[-1] in punctuations:
        x = x[:-1]
    return x

def droprare(x):
    if (x[1] > 1):
        return x

# too slow
def books2words(rdd, stopwords, punctuations):
    # x[0] are the file names x[1] are contents
    if stopwords:
        rdd = rdd.flatMap(lambda x: x[1].lower().split()).filter(lambda x: x not in stopwords)
        if punctuations:
            rdd = rdd.filter(lambda x: length(x)).map(lambda x: punct(x))
        return rdd
    else:
        return rdd.flatMap(lambda x: x[1].lower().split())

def countwords(rdd):
    return rdd.map(lambda x: (x, 1)).reduceByKey(add).filter(lambda x: droprare(x))

def text_processing(sc, file_path, stop_words, punctuations=True, select_top=False):
    if os.path.isdir(file_path):  # dir or a file indicator
        file_path = file_path + '/*'

    if stop_words:  # user chooses to remove stop words or not
        stop_words = sc.textFile(stop_words, encoding='utf-8').collect()

    all_files = sc.wholeTextFiles(file_path)  # accepts dir or file
    all_books = all_files.filter(lambda x: (not ("stopwords.txt" in x[0])))  # drop stopwords doc
    all_words = books2words(all_books, stop_words, punctuations)  # flatmaps books to word list
    # only if punct is active
    if select_top:
        return countwords(all_words).sortBy(lambda x: x[1], ascending=False).take(select_top)
    else:
        return countwords(all_words)