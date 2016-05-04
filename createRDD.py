#conding:utf-8
from pyspark import *
from pyspark.streaming import StreamingContext

#创建RDD有两种方式：1.读取外部数据集;2.在驱动器程序中对一个集合进行并行化(传给SparkContext的parallelize()方法)
#将程序中已有的集合，传给SparkContext的parallelize()方法

sc = SparkContext("local", "parallizeApp")
items = sc.parallize(["pandas", "I like pandas"])

