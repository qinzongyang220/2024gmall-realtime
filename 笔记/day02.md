对mr,mapjoin的工作原理有更深的了解，编写逐字稿，观看尚硅谷的10-18的实时电商数仓的讲解

er模型和维度建模（维度选择星型模型，还是雪花模型）

现在企业大多使用雪花模型，因为他们彼此之间有联系，

事实表和维度表

书写电商数仓的项目架构以及代码版本的统一，为了减少依赖冲突，hadoop我选择的是3.3.4版本，而java是之前离线数仓时已经搭建好的1.8版本的，不需要重新搭建

有的同事指出flinkcdc读取不到mysql是因为mysql的版本过低，是5.7的，而没有问题的同学大多都是8.0的，为了尝试解决依赖问题，对mysql进行更新，但失败了，中途，有更好的解决办法，对maven进行更换

对maven的依赖进行更换

# 数据倾斜

import pandas as pd
import random
import string


def generate_userid():
	return ''.join(random.choices(string.digits, k=8))


def generate_order_no():
	return ''.join(random.choices(string.digits, k=10))


def generate_product_no():
	return ''.join(random.choices(string.digits, k=6))


def generate_color_no():
	return ''.join(random.choices(string.digits, k=3))




def generate_sale_amount():
	return round(random.uniform(0, 1000), 2)


# 生成华东地区的数据
data_east = []
for _ in range(80000000):
	data_east.append([
		generate_userid(),
		generate_order_no(),
		"华东",
		generate_product_no(),
		generate_color_no(),
		generate_sale_amount()
	])
df_east = pd.DataFrame(data_east, columns=["userid", "order_no", "region", "product_no", "color_no", "sale_amount"])

# 生成西北地区的数据
data_west = []
for _ in range(20000000):
	data_west.append([
		generate_userid(),
		generate_order_no(),
		"西北",
		generate_product_no(),
		generate_color_no(),
		generate_sale_amount()
	])
df_west = pd.DataFrame(data_west, columns=["userid", "order_no", "region", "product_no", "color_no", "sale_amount"])

# 合并华东和西北的数据
result_df = pd.concat([df_east, df_west], ignore_index=True)
print(result_df)

​    