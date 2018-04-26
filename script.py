from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import time
start_time = time.time()
conf = SparkConf().setAppName("Hard Drive")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("Hard Drive").getOrCreate()
#fnames = sc.textFile("file:///home/tnguyen/BigData/project4/2017-*.csv")
fnames = sc.textFile("file:////bigdata/data/backblaze/2017/2017-*.csv")
m = fnames.map(lambda line: line.split(","))

def getfailure(rows):
    s = []
    s.append((rows[0],rows[1],rows[4])) 
    return s
rows = m.flatMap(getfailure)
header = rows.first()
rows = rows.filter(lambda line: line!=header)
#print(rows.collect())
r = rows.toDF(header)
#r.show()
su = r.groupby("date","failure").count()
#su.show()
su = su.withColumn('day',lit(1))
#su.show()
#doing the fail rate
fail = su.where(su["failure"]==1)
fail_agg = fail.groupby().sum()
av = fail_agg['sum(count)']/fail_agg['sum(day)']
fail_agg = fail_agg.withColumn('avg_failure',av)
fail_agg.toPandas().to_csv("fail_avg.csv")

#doing the avg number of hard drive
my_window = Window.partitionBy().orderBy("date")

drive =su.where(su["failure"]==0)
drive = drive.orderBy("date")
#drive.show()
drive = drive.withColumn("pre_drives", F.lag(drive["count"]).over(my_window))
drive = drive.withColumn("diff",F.when(F.isnull(drive["count"]-drive["pre_drives"]),0).otherwise(drive["count"]-drive["pre_drives"]))
#drive.show()
drive_agg = drive.groupby().sum()
av2 = drive_agg['sum(diff)']/drive_agg['sum(day)']
drive_agg = drive_agg.withColumn('avg_drives',av2)
drive_agg.toPandas().to_csv("drive_agg.csv")
print("--- %s seconds ----" %(time.time()-start_time))
#https://www.arundhaj.com/blog/calculate-difference-with-previous-row-in-pyspark.html
