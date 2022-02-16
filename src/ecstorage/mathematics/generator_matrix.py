# 生成矩阵
import numpy as np
from . import galois

'''
范德蒙德矩阵
'''
def vander_matrix(k,m):
    data = np.arange(1,k+1,1)
    return np.vander(data,m).transpose()

'''
柯西矩阵
'''
def cauchy_matrix(data):

    k = len(data)
    x = np.random.randint(0,16,k) #生成元素个数为k，范围在[0,16)的数组
    y = np.random.randint(0,16,k)

    ## 伽罗华域优化
    # gf = galois.GF(16)
    # for i in range(k):
    #     for j in range(k):
    #         cauchy[i][j] = gf.mul(int(x[i]),int(y[j]))
    #         # print(np.linalg.det(cauchy))

    return np.subtract.outer(x, y)

    '''
    单位矩阵
    '''
    # def identity_matrix(data):
    #     return np.identity(len(data))