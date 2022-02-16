import numpy as np
# (numpy array格式)稠密矩阵转稀疏矩阵
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

