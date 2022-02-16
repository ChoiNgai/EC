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
data = sc.parallelize(sparse(data))
k = data.count()

check_block = ec.reedsolomon(sc,data,m,generator_matrix)

# test
data[0] = None          # 缺失数据（缺失个数小于等于m）
data[1] = None
data[2] = None
print(data)
# check_block[1] = None

# 恢复数据
recover_data = ec.verify(data,check_block,generator_matrix)
print(recover_data)
