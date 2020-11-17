from pyspark import SparkContext
import sys
import csv

if __name__=='__main__':
    def parseCSV(idx, part):
        if idx == 0:
            next(part)
        for p in csv.reader(part):
            yield ("\""+p[1]+"\"", p[0].split("-")[0], p[7])

    path_in = sys.argv[1] if len(sys.argv)>1 else "complaints_small.csv"
    path_out = sys.argv[2] if len(sys.argv)>2 else "hw4_output"
    sc = SparkContext.getOrCreate()

    rows= sc.textFile(path_in, use_unicode=False).mapPartitionsWithIndex(parseCSV)\
            .map(lambda x: ((x[0], x[1], x[2]), 1))\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x:(x[0][:2],x[1]))\
            .groupByKey()\
            .mapValues(lambda x: (str(sum(x)), str(len(x)), str(int(float(max(x))/sum(x)*100))))\
            .sortByKey()\
            .map(lambda x:(x[0]+x[1]))\
            .map(lambda x: (", ".join(x)))\
            .collect()
    sc.parallelize(rows).saveAsTextFile(path_out)

