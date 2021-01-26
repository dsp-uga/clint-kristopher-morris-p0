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
import json
from p0_tools import text_processing
from absl import app, flags
from absl.flags import FLAGS
from string import punctuation

flags.DEFINE_string('file', './data', 'path to book file(s)')
flags.DEFINE_string('stopwords', '','path to stopwords txt file')
flags.DEFINE_integer('top', 40, 'number to top words to return')
flags.DEFINE_boolean('punctuations', False, 'remove leading and trailing punctuations')
flags.DEFINE_string('outfile', './result/demo.json','where the result is saved')

def main(_argv):
    # conf spark
    conf = (SparkConf().setMaster("local").setAppName("SubProjectB"))
    sc = SparkContext(conf = conf)

    counted_words = text_processing(sc,
                                    FLAGS.file,
                                    FLAGS.stopwords,
                                    FLAGS.punctuations,
                                    select_top=FLAGS.top)

    output_dict = sc.parallelize(counted_words).collectAsMap()
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