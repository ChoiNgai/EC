import numpy as np
# (numpy array格式)稠密矩阵 转 稀疏矩阵
def sparse(data):
    
    if type(data) == list:
        data = np.array(data,ndmin=2)
    elif data.ndim == 1:
        data = np.array(data,ndmin=2)
        data = data.transpose()
    sparse_matrix = []

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            if data[i][j] != 0:
                sparse_matrix.append((i,j,data[i][j]))
    return sparse_matrix

# (numpy array格式)稀疏矩阵 转 稠密矩阵
# 注:只支持以行数小的在前面这种格式的转换,例如支持[(0,0,0), (1, 0, 0)], 不支持[(1,0,0), (0,0,0)],即输入稀疏矩阵的排序规则需要符合order by row,col
def dense(data):
    dense_matrix = np.array([])
    row = 0         #行索引
    tmp_row = []    #保存同一行的值
    for i in range(len(data)):
        if data[i][0] == row:
            tmp_row.append(data[i][2])
        else:
            dense_matrix = np.insert(tmp_row, 0, values=dense_matrix, axis=0)
            row += 1
            tmp_row = []
    dense_matrix = np.insert(tmp_row, 0, values=dense_matrix, axis=0)
    return dense_matrix

'''
将元素全为MatrixEntrytoArray类型的 RDD 转为 array
'''
def MatrixEntrytoArray(data):
    data = data.collect()
    data_new = []
    for i in range(len(data)):
        data_new.append( tuple(str(data[i]).replace('MatrixEntry','')) )