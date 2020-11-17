from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import sys
import csv
if __name__=='__main__':
    def parseCSV(idx, part):
        if idx == 0:
            next(part)
        for p in csv.reader(part):
            yield ("\""+p[1]+"\"", p[0].split("-")[0], p[7])
        
    path_in = "complaints_small.csv" if len(sys.argv)>1 else sys.argv[1]  
    path_out = "hw4_output" if len(sys.argv)>2 else sys.argv[2] 
    sc = SparkContext.getOrCreate()
    #sqlContext = SQLContext(sc)
    rows= sc.textFile(path_in).mapPartitionsWithIndex(parseCSV)\
            .map(lambda x: ((x[0], x[1], x[2]), 1))\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x:(x[0][:2],x[1]))\
            .groupByKey()\
            .mapValues(lambda x: (str(sum(x)), str(len(x)), str(round((max(x)/sum(x)*100)))))\
            .sortByKey()\
            .map(lambda x:(x[0]+x[1]))\
            .map(lambda x: (", ".join(x)))\
            .collect()
    for i in rows:
        print(i)
    #sc.parallelize(rows).saveAsTextFile(path_out)
