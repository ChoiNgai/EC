import sys
sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import *
import ecstorage.mathematics.generator_matrix as generator
from ecstorage.mathematics.matrix_optimization import sparse

'''
创建生成矩阵
输入:
    k:      数据个数
    m:      校验块个数
输出:
    generator_matrix: 生成矩阵
'''
def generator_matrix_init(k,m,generator_method='vander'):
    if generator_method == 'cauchy':
        A = generator.cauchy_matrix(data,m)   #柯西矩阵(未实现)
    elif generator_method == 'vander':
        A = generator.vander_matrix(k,m)      #范德蒙德矩阵

    else:
        print("error")

    generator_matrix = np.array(np.concatenate((np.mat(np.identity(k)), A), axis=0))
    
    return generator_matrix

'''
把数值修改成None
输入:
    check_data: 包含校验块的缺失数据
    loss_idx:   缺失数据的索引
    m:          校验块个数
输出:
    check_data: 相比输入更多缺失的数据（缺失个数为m）
    loss_idx:   输出check_data缺失数据的索引
'''
def none_enough(check_data,loss_idx,m):
    loss_idx = loss_idx.tolist()
    i = 0
    while len(loss_idx) < m:
        check_data[i] = None
        loss_idx = np.where(np.array(check_data) == None)[0]
        i = i + 1
    return check_data,loss_idx
'''
生成校验块
输入:
    sc:     SparkContext
    data:   输入数据 (rdd格式数据)
    m:      校验块个数 (python int型数据)
输出:
    generator_matrix: 生成矩阵
    generator_matrix.dot(data): 数据块+校验块
'''
def reedsolomon(sc,data,m,generator_matrix_case = 'cauchy',):

    k = data.count()
    
    #创建SparkContext
    # sc = SparkContext()

    # 产生生成矩阵
    generator_matrix = generator_matrix_init(k,m)

    generator_matrix = sc.parallelize(sparse(generator_matrix))     #将稠密矩阵转换为稀疏矩阵并创建RDD

    data = CoordinateMatrix(data).toBlockMatrix()
    generator_matrix = CoordinateMatrix(generator_matrix).toBlockMatrix()
    check_block = generator_matrix.multiply(data).toCoordinateMatrix().entries.collect()

    return check_block

'''
恢复数据
输入:
    loss_data:
    check_block:
    generator_matrix_case: 生成矩阵采用的方法(缺省值),默认是 'cauchy'
    arraytype:  数组计算类型(缺省值),默认是 'int'（浮点数计算可能结果错误）
    outtype: 输出类型(缺省值),默认是list类型
'''
def verify(loss_data,check_block,generator_matrix_case = 'cauchy',arraytype = 'int',outtype='list'):

    k = len(loss_data)
    m = len(check_block)

    # 生成矩阵
    generator_matrix = generator_matrix_init(k,m)

    # 删除生成矩阵(generator_matrix) 中对应缺失数据的行 
    check_data = loss_data + check_block
    
    loss_idx = np.where(np.array(check_data) == None)[0]

    # 如果None不够就删掉一些好让后续生成矩阵是方阵求逆
    check_data,loss_idx = none_enough(check_data,loss_idx,m)
            
    generator_matrix = np.delete(generator_matrix,loss_idx, axis = 0)

    # 删除数据中值为None的数据
    check_data = np.delete(check_data,loss_idx, axis = 0)

    if arraytype == 'int':
        generator_matrix = generator_matrix.astype(int)
        check_data = check_data.astype(int)
        recover_data = np.linalg.inv(generator_matrix).dot(check_data).astype(int)
    else:
        recover_data = np.linalg.inv(generator_matrix).dot(check_data)

    if outtype == 'list':
        recover_data = recover_data.tolist()[0]

    return recover_data

# # (numpy array格式)稠密矩阵转稀疏矩阵
# def sparse(data):
#     sparse_matrix = []
#     i = j = 0
#     for i in range(data.shape[0]):
#         for j in range(data.shape[1]):
#             sparse_matrix.append((i,j,data[i][j]))
#     return sparse_matrix