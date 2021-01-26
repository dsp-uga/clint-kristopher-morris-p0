# import items and set paths to local spark
import os
import sys
spark_path = r"/home/clint/spark-3.0.1-bin-hadoop2.7" # spark installed folder
os.environ['SPARK_HOME'] = spark_path
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/'
sys.path.insert(0, spark_path + "/bin")
sys.path.insert(0, spark_path + "/python/pyspark/")
sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9-src.zip")
from operator import *
from pyspark import *
from string import punctuation
import json
from p0_tools import text_processing
from absl import app, flags
from absl.flags import FLAGS
import math

flags.DEFINE_string('file', './data', 'path to book file(s)')
flags.DEFINE_string('stopwords', './data/stopwords.txt','path to stopwords txt file')
flags.DEFINE_integer('top', None, 'number to top words to return')
flags.DEFINE_boolean('punctuations', True, 'remove leading and trailing punctuations')
flags.DEFINE_string('outfile', './result/demo.json','where the result is saved')

def main(_argv):
    # conf spark
    conf = (SparkConf().setMaster("local").setAppName("SubProjectB"))
    sc = SparkContext(conf = conf)

    book_files = os.listdir(FLAGS.file)
    book_files.remove(FLAGS.stopwords.replace(f'{FLAGS.file}/', ''))
    books = {}
    # creates a RDD for each book
    for file in book_files:
        books[f'{file}'] = text_processing(sc,
                                           (f'{FLAGS.file}/{file}'),
                                           FLAGS.stopwords,
                                           FLAGS.punctuations,
                                           select_top=FLAGS.top)

    # creates a dict of {key='word', val=nt}
    booknames = list(books.values())
    v2 = booknames.pop(0).map(lambda x: (x[0]))
    for v in booknames:
        v = v.map(lambda x: (x[0]))
        v2 = v2.union(v)  # union keeps RDD form
    nt_dict = v2.map(lambda x: (x, 1)).reduceByKey(add).collectAsMap()

    scores = {}
    output = []
    # creates a dict of {key='book file name', val=top 5 TF-IDF}
    for key, RDD in books.items():
        scores[key] = RDD.map(lambda x: (x[0], x[1] * math.log(8/(nt_dict[x[0]]))))\
            .sortBy(lambda x: x[1], ascending=False).take(5)
        output = output + scores[key]

    output_dict = sc.parallelize(output).collectAsMap()

    # Write to a JSON files
    with open(FLAGS.outfile, 'w') as f: json.dump(output_dict, f)
    f.close()
    # End Spark Session
    sc.stop()


if __name__ == '__main__':
    try:
        app.run(main)
    except SystemExit:
        pass