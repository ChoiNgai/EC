# 文件夹测试导入
import sys
sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

# 导入模块
import ecstorage.list as ec
import numpy as np

m = 3                       #生成校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

data = [1, 0, 0, 8, 6]
# data = np.arange(1,6,1)
k = len(data)

check_block = ec.reedsolomon(data,m,generator_matrix)

# test
data[0] = None          # 缺失数据（缺失个数小于等于m）
data[1] = None
data[2] = None
print(data)
# check_block[1] = None

# 恢复数据
recover_data = ec.verify(data,check_block,generator_matrix)
print(recover_data)