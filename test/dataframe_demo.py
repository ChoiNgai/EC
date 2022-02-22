
# 文件夹测试导入
import sys
sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

# 导入模块
import ecstorage.dataframe as ec
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
from ecstorage.mathematics.matrix_optimization import sparse
import numpy as np
import os
os.environ["PYSPARK_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"

from pyspark.sql import SQLContext
m = 3                       #生成校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

sc = SparkContext()
sqlContext = SQLContext(sc)

dicts = [
        {'col1':'a', 'col2':1},
        {'col1':'b', 'col2':2},
        {'col1':'b', 'col2':3},
        {'col1':'b', 'col2':4},
        {'col1':'b', 'col2':5},
         ]
df = sqlContext.createDataFrame(dicts)
data = df.select('col2')
# data.show()

check_block = ec.reedsolomon(sc,data,m)
check_block.show()


# 测试
dicts = [
        {'col1':'a', 'col2':None},
        {'col1':'b', 'col2':None},
        {'col1':'b', 'col2':3},
        {'col1':'b', 'col2':4},
        {'col1':'b', 'col2':5},
         ]
# # data[2] = None
# check_block = check_block.collect()
# check_block[0] = None
# check_block = sc.parallelize(check_block)
data = sqlContext.createDataFrame(dicts)
data = data.select('col2')
data.show()
# 恢复数据
recover_data = ec.verify(sc,data,check_block,generator_matrix)  
recover_data.show()