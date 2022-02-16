from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
import numpy as np
from scipy import sparse
from pyspark.mllib.linalg import SparseVector, DenseVector
import os 
os.environ["PYSPARK_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"
 
ss = SparkSession.builder.appName("test") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
 
sc = ss.sparkContext
sc.setLogLevel("WARN")
 
# # M_rdd = sc.parallelize([(0,0, 1), (0,1, 2), (0,2, 3), (1,0, 4), (1,1, 5), (1,2, 6)])
# # N_rdd = sc.parallelize([(0,0, 7), (0,1, 8), (1,0, 9), (1,1, 10), (2,0, 11), (2,1, 12)])
# # M = CoordinateMatrix(M_rdd).toBlockMatrix()
# # N = CoordinateMatrix(N_rdd).toBlockMatrix()
# # C = M.multiply(N).toCoordinateMatrix().entries.collect()

# data = np.arange(1,6,1)
# M = np.vander(data,5).transpose()
# M=sparse.csr_matrix(M)
# M = sc.parallelize(M)
# M = CoordinateMatrix(M).toBlockMatrix()
# N = M
# C = M.multiply(N).toCoordinateMatrix().entries.collect()
# print(C)

# # 数组转成稀疏矩阵
# def sparse(p):
#     vec=[int(x) for x in p[2:]]
#     lvec=len(vec)
#     dic1={}
#     for i in range(lvec):
#         if vec[i]==1:
#             dic1[i]=1
#     return [p[0],p[1],SparseVector(lvec,dic1)]

# data = np.arange(1,6,1)
# M = np.vander(data,5).transpose()
# # M = sc.parallelize(M)
# M = sparse(M)
# N = M

# M = CoordinateMatrix(M).toBlockMatrix()
# N = CoordinateMatrix(N).toBlockMatrix()

# C = M.multiply(N).toCoordinateMatrix().entries.collect()
# print(C)

import numpy as np
# data = np.arange(1,6,1)
# data = np.vander(data,5).transpose()
data = np.arange(6).reshape(2,3)

# (numpy array格式)稠密矩阵转稀疏矩阵
def sparse(data):
    sparse_matrix = []
    i = j = 0
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            sparse_matrix.append((i,j,data[i][j]))
    return sparse_matrix

A = sparse(data)
A = sc.parallelize(A)
print(A)

    