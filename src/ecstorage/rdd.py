import numpy as np
from pyspark.mllib.linalg.distributed import *
from ecstorage.mathematics.generator_matrix import generator
from ecstorage.mathematics.matrix_optimization import sparse
from ecstorage.mathematics.matrix_optimization import dense

'''
把数值修改成None
输入:
    check_data: 包含校验块的缺失数据(np.array)
    loss_idx:   缺失数据的索引(np.array)
    m:          校验块个数
输出:
    check_data: 相比输入更多缺失的数据（缺失个数为m）
    loss_idx:   输出check_data缺失数据的索引
'''
def none_enough(check_data,loss_idx,m):
    loss_idx = loss_idx.tolist()
    # check_data.filter(lambda data:data.collect()[i][0] != None for i in range(len(loss_idx))  )
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

    # 产生生成矩阵
    generator_matrix = np.array(generator(k,m))

    generator_matrix = sc.parallelize(sparse(generator_matrix))     #将稠密矩阵转换为稀疏矩阵并创建RDD

    data = CoordinateMatrix(data).toBlockMatrix()
    generator_matrix = CoordinateMatrix(generator_matrix).toBlockMatrix()
    check_block = generator_matrix.multiply(data).toCoordinateMatrix().entries.collect()    # MatrixEntry格式
    
    return sc.parallelize(check_block[-m:])

'''
恢复数据
输入:
    loss_data:
    check_block:
    generator_matrix_case: 生成矩阵采用的方法(缺省值),默认是 'cauchy'
    arraytype:  数组计算类型(缺省值),默认是 'int'（浮点数计算可能结果错误）
    outtype: 输出类型(缺省值),默认是list类型
'''
def verify(sc,loss_data,check_block,generator_matrix_case = 'cauchy',arraytype = 'int',outtype='list'):

    k = loss_data.count()
    m = check_block.count()
    check_bolck = check_bolck.toRowMatrix()

    # 生成矩阵
    generator_matrix = generator(k,m)

    # 删除生成矩阵(generator_matrix) 中对应缺失数据的行 
    check_data = loss_data.union(check_block)   #写到这（明天继续）

    check_data = dense(check_data.collect())   #稀疏矩阵格式 转 稠密矩阵格式
    # check_data = loss_data + check_block

    loss_idx = np.where(np.array(check_data) == None)[0]

    # 如果None不够就删掉一些好让后续生成矩阵是方阵求逆
    check_data,loss_idx = none_enough(np.array(check_data),loss_idx,m)
            
    generator_matrix = np.delete(generator_matrix,loss_idx, axis = 0)

    # 删除数据中值为None的数据
    check_data = np.delete(check_data,loss_idx, axis = 0)

    generator_matrix = sc.parallelize(sparse( np.linalg.inv(generator_matrix) ))     #将稠密矩阵求逆后转换为稀疏矩阵并创建RDD
    check_data = sc.parallelize(sparse(check_data))

    generator_matrix = CoordinateMatrix(generator_matrix).toBlockMatrix()
    check_data = CoordinateMatrix(check_data).toBlockMatrix()

    recover_data = generator_matrix.multiply(check_data).toCoordinateMatrix().entries.collect()

    return recover_data