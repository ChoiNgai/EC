# 文件夹测试导入
import sys
sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

# 导入模块
import ecstorage.rdd as ec
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
from ecstorage.mathematics.matrix_optimization import sparse
import numpy as np
import os
os.environ["PYSPARK_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"

m = 3                       #生成校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

sc = SparkContext()
# sc.setLogLevel("WARN")
spark = SparkSession(sc)
data = np.arange(1,6,1)

# e
data = sc.parallelize(sparse(data))
k = data.count()

check_block = ec.reedsolomon(sc,data,m,generator_matrix)

print(check_block.collect())
# test
data = list(np.arange(1,6,1))
print(data)
data[0] = None          # 缺失数据（缺失个数小于等于m）
data[1] = None
data[2] = None
# check_block[1] = None
data = sc.parallelize(sparse(data))
# 恢复数据
recover_data = ec.verify(sc,data,check_block,generator_matrix)  
print(recover_data)

# 存在的问题：
# 1）计算不准确（结果返回为浮点型，且数值上有 10^-14 大小左右的偏差 ）
# 2）reedsolomon方法中返回的check_block是没有坐标的rdd格式更为合适，即块多少个，就多少个数值