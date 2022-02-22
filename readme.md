# EC
[EC存储（ec-storage）](https://pypi.org/project/ec-storage/)

## file

> 源码文件目录（src/ecstorage）：
>
> - __init\_\_.py                        
> - mathematics                 数学函数
>      - __init\_\_.py				
>      - galois.py              伽罗华域运算
>      - generator_matrix.py    生成矩阵 
>      - matrix_optimization.py   矩阵优化
> - list.py           list格式计算
> - rdd.py          rdd格式计算
> - dataframe.py   dataframe格式计算（开发中）

## install

```shell
pip install ec-storage
```

## manual

### list格式计算（单机版）

```python
# 本地文件夹测试导入
# import sys
# sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

# 导入模块
import ecstorage.list as ec
import numpy as np

m = 3                       #选择校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

data = [1, 0, 0, 8, 6]			#list格式数据
k = len(data)

check_block = ec.reedsolomon(data,m,generator_matrix)	#生成校验块(list格式)

# 测试（数据缺失个数+校验块缺失个数 <= m）
data[0] = None          # 缺失数据
data[1] = None
check_block[1] = None		#校验块也可以缺失
print(data)

# 恢复数据
recover_data = ec.verify(data,check_block,generator_matrix)	#恢复数据(list格式)
print(recover_data)		#[1, 0, 0, 8, 6]
```



### RDD格式计算（分布式版）

```python
# #本地文件夹测试导入
# import sys
# sys.path.append("/Users/caiwei/Documents/code/EC-dev/src")

# #统一python版本(有多个python版本的情况下)
# import os
# os.environ["PYSPARK_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/caiwei/opt/anaconda3/bin/python"

# 导入必要的模块
import ecstorage.rdd as ec
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
from ecstorage.mathematics.matrix_optimization import sparse
import numpy as np

m = 3                       #校验块个数
generator_matrix = 'vander' #生成矩阵选择范德蒙德矩阵

# 创建spark session
sc = SparkContext()
spark = SparkSession(sc)

# 数据
data = np.arange(1,6,1)
data = sc.parallelize(data)	#数据转为rdd格式

# 生成校验块
check_block = ec.reedsolomon(sc,data,m,generator_matrix)

# 测试（数据缺失个数+校验块缺失个数 <= m）
data = list(np.arange(1,6,1))
data[0] = None          # 缺失数据（缺失个数小于等于m）
data[1] = None
# data[2] = None


# 也可以是校验块有缺失数据
check_block = check_block.collect()
check_block[0] = None
check_block = sc.parallelize(check_block)

# 恢复数据
recover_data = ec.verify(sc,data,check_block,generator_matrix)  
print(recover_data.collect())

```

