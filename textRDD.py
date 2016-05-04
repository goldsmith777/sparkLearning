#coding:utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from numpy import *
import time

# 每个spark应用都由一个驱动器程序来驱动，用于发起并行操作
# python程序、shell本身都可以作为驱动器程序。

sc = SparkContext("local", "firstApp")
# 驱动器程序通过sparkContext对象来访问spark
distFile = sc.textFile("c:\Hadoop_data\lib\data.txt")
# 对一个文本文档生成一个RDD
# RDD支持两种类型的操作：转化操作（transformation）和行动操作（action），转化操作会生成新的RDD，行动操作会对RDD计算出一个结果
longestLine = distFile.map(lambda lines: len(lines)).reduce(lambda a,b: a if(a>b) else b)
# 转化操作：map是并行化操作命令，操作的对象是文本的每一行。行动操作：reduce是把每一行都加进来计算一遍。
pythonFilter = distFile.filter(lambda lines:'s' in lines)
# 转化操作：并行执行一个过滤操作，返回符合条件的一个RDD子集
count = distFile.count()
# 行动操作：统计RDD中元素的个数
# print count, longestLine, pythonFilter.reduce(lambda a, b: sorted([a, b])[0])
enFilter = distFile.filter(lambda item: 'learning' in item)
cnFilter = distFile.filter(lambda item: u'学习' in item)
cnEnFilter = cnFilter.union(enFilter)
# union是转化操作，将两个RDD组合起来
for line in cnEnFilter.take(cnEnFilter.count()): # 打印出整个RDD所有的元素
    print line


