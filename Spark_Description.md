## 1. 在Spark上实现中文词频统计。
### 源码在homework9/master/source/testwordcount.scala
&ensp;&ensp;在IntelliJ IDEA上用scala实现  
&ensp;&ensp;结果截图  
![Image text](https://raw.github.com/cjjloves/homework9/master/screenshot/result.JPG)  
### 遇到的问题
&ensp;&ensp;1. scala与spark版本不兼容问题  
![Image text](https://raw.github.com/cjjloves/homework9/master/problems/版本不兼容.JPG)  
&ensp;&ensp;解决方法是查询文档，得到兼容的scala版本  
![Image text](https://raw.github.com/cjjloves/homework9/master/problems/版本不兼容2.JPG) 
## 2. 使用Spark SQL查询统计结果中次数超过k次的词语。
### 源码在homework9/master/source/SparkSql.scala
&ensp;&ensp;预处理：将键值对(key,value)通过命令行处理替换成(key,value，即删掉最后的“)”，以便适应Spark SQL输入格式  
&ensp;&ensp;k=5000  
![Image text](https://raw.github.com/cjjloves/homework9/master/screenshot/sql-result.JPG)  
