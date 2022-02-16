# EC
[EC存储（ec-storage）](https://pypi.org/project/ec-storage/)

## 源码

> 源码文件目录（src/ecstorage）：
>  
> - __init__.py                  主文件
> - mathematics                 数据函数
>      - galois.py              伽罗华域运算
>      - generator_matrix.py    生成矩阵 

## 安装

```shell
pip install ec-storage
```

## demo

```python
import ecstorage

m = 3                       #生成校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

data = [1, 0, 0, 8, 6]
k = len(data)

check_block = ecstorage.reedsolomon(data,m,generator_matrix)

# test
data[0] = None          # 缺失数据（缺失个数小于等于m）
data[1] = None
data[2] = None
# data: [None, None, None, 8, 6]

# 恢复数据
recover_data = ecstorage.verify(data,check_block,generator_matrix)
print(recover_data)
# recover_data: [1, 0, 0, 8, 6]
```

