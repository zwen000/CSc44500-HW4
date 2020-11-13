
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import sys
if __name__=='__main__':
    
    path_in = sys.argv[1] if len(sys.argv)>1 else "complaints_small.csv"
    path_out = sys.argv[2] if len(sys.argv)>2 else "hw4_output"
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    rows= sc.textFile(path_in).mapPartitionsWithIndex(parseCSV)\
            .map(lambda x: ((x[0], x[1], x[2]), 1))\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x:(x[0][:2],x[1]))\
            .groupByKey()\
            .mapValues(lambda x: (sum(x), len(x),round((max(x)/sum( x)*100))))\
            .sortByKey()\
            .map(lambda x:(x[0]+x[1]))\
            .saveAsTextFile(path_out)

