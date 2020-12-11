#!/usr/bin/env python3


from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql.functions import col,udf,unix_timestamp
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("job-1").master("local[*]").getOrCreate()


communes_df = spark.read.option("header",True).option("delimiter", ";").csv("/home/kmossaab/esgi_tp/Communes.csv")
communes_df1 = communes_df.select("DEPCOM","PTOT")
communes_df1.show()

cp_df = spark.read.option("header",True).option("delimiter", ";").csv("/home/kmossaab/esgi_tp/code-insee-postaux-geoflar.csv")
cp_df1 = cp_df.select("CODE INSEE","CODE INSEE","geom_x_y")
cp_df1.show()

poste_synop_df = spark.read.option("header",True).option("delimiter", ";").csv("/home/kmossaab/esgi_tp/postesSynop.txt")
poste_synop_df1 = poste_synop_df.select("ID","Latitude","Longitude")
poste_synop_df1.show()

synop_df = spark.read.option("header",True).option("delimiter", ";").csv("/home/kmossaab/esgi_tp/synop.2020120512.txt")
synop_df1 = synop_df.select("t","numer_sta","date")


synop_df2 = synop_df1.withColumn('temperature', synop_df1.t-273.15 )


synop_df2 = synop_df2.withColumnRenamed("numer_sta","ID")
synop_df2.show()

df = poste_synop_df1.join(synop_df2, poste_synop_df1.ID == synop_df2.ID)
df = df.drop(synop_df2.ID)
df = df.drop(synop_df2.t)
df = df.withColumnRenamed("ID","id_station")
df.show()

func =  udf(lambda x: datetime.strptime(str(x), '%m%d%y'), DateType())

df2 = df.withColumn('date', func(col('InvcDate')))
