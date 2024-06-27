# spark分布式解压工具

[TOC]



## 一、目标

​    spark解压缩工具，目前支持tar、gz、zip、bz2、7z压缩格式，默认解压到当前路下，也支持自定义的解压输出路径。另外支持多种提交模式，进行解压任务，可通过自定义配置文件，作为spark任务的资源设定

## 二、详细设计

2.1 使用hadoop的FileSystem类，对tos文件的进行读取、查找、写入等操作
2.2 获取到tos文件或目录，对压缩文件进行解压，解压成字节数组，以流的方式写入tos文件系统
2.3 使用maven工具打包，单独生成一个spark配置文件(提交spark作业时进行资源的分配)，每次启动spark job时，会加载这个自定义配置文件
2.4 封装到shell脚本，通过sparktar命令执行解压

## 三、操作说明

### 1.提交模式

支持3种spark的提交模式，建议7z解压用yarn-client|cluster模式提交，主要会涉及到写磁盘的流程，避免把单节点磁盘打满的风险

|               | tar  | zip  | bz2  | 7z   |
| ------------- | ---- | ---- | ---- | ---- |
| 本地local模式 | 支持 | 支持 | 支持 | 支持 |
| yarn-client   | 支持 | 支持 | 支持 | 支持 |
| yarn-cluster  | 支持 | 支持 | 支持 | 支持 |



### 2.压缩输出路径支持2种方式

a)默认直接解压到当前路径，不会覆盖原始压缩文件；
b)还可以指定解压输出路径

### 3.操作命令及说明

#### 3.1操作命令

目前已在172.xxx.xxx.xxx机器配置好了，可通过 sparktar 命令来执行解压操作
sparktar -cluster tos://report/tmp/tar/ tos://report/tmptar/ tmp.produce.properties

#### 3.2命令说明

sparktar 提交方式 解压路径 [解压输出路径] 配置文件
提交方式, 必选参数，提交可选3种方式:  -client, -local, -cluster
解压路径: 必选参数，支持输入路径和文件绝对路径, 比如tos://report/tmp/或tos://report/tmp/xx.gz
输出路径: 非必选参数，默认解压到当前路径下
配置文件: 必选参数，约定在/bin/spark-tar/config下创建, 文件格式xxx.produce.properties(后缀名统一写成produce.properties, xxx前缀自定义写，最好是见名知意，后面xxx作为spark作业名称)

## 四、操作案例

#### 4.1案例1

yarn-cluster模式，将tos://report/tmp/下的压缩包，解压到当前路径
第一步：在/bin/spark-tar/config路径下编写配置文件
vim /bin/spark-tar/config/tmp.produce.properties

```properties
# spark.driver内存
spark.driver.memory=1g

# spark.driver的核数
spark.driver.cores=2

# spark.executor的核数,官网推荐4~5个
spark.executor.cores=4

# spark.executor内存,大概1个yarn节点总内存/1个yarn节点的excuter数,具体还需要结合job的数据集以及划分并行度情况设定
spark.executor.memory=4g

# executor的个数
spark.executor.instances=4

# yarn.executor堆外内存
spark.yarn.executor.memoryOverhead=1g
```
