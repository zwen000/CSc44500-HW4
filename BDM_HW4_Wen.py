from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import sys
import csv



def main(sc):
    def parseCSV(idx, part):
        if idx == 0:
            next(part)
		r = csv.reader(part)
        for p in r:
            yield (p[1],p[0].split("-")[0], p[7])

    
    #sqlContext = SparkSession.builder.getOrCreate()
    path_in = "complaints_small.csv"
    path_out = "hw4_output"
    if len(sys.argv)>1:
        path_in = sys.argv[1]
    if len(sys.argv)>2:
        path_out = sys.argv[2]
        
    
    rows= sc.textFile(path_in)\
            .mapPartitionsWithIndex(parseCSV)\
            .map(lambda x: ((x[0], x[1], x[2]), 1))\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x:(x[0][:2],x[1]))\
            .groupByKey()\
            .mapValues(lambda x: (sum(x), len(x),round((max(x)/sum( x)*100))))\
            .sortByKey()\
            .map(lambda x:(x[0]+x[1])).collect()
    sc.parallelize(rows).saveAsTextFile(path_out)
	
	
#     for i in rows.collect():
#         print(i)
#     df = sqlContext.createDataFrame(rows)
#     df.write.csv(path_out, sep=",")

	print("done")









if __name__=='__main__':
	sc = SparkContext.getOrCreate()
	main(sc)
#spark-submit --num-executors 2 --executor-cores 5 BDM_HW4_Wen.py /tmp/bdm/complaints.csv output_folder
