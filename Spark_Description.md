## 1. 在Spark上实现中文词频统计。
&ensp;&ensp;在IntelliJ IDEA上用scala实现  
&ensp;&ensp;结果截图  
![Image text](https://raw.github.com/cjjloves/homework9/master/screenshot/result.JPG)  
### 遇到的问题
&ensp;&ensp;1. scala与spark版本不兼容问题  
![Image text](https://raw.github.com/cjjloves/homework9/master/problems/版本不兼容.JPG)  
&ensp;&ensp;解决方法是查询文档，得到兼容的scala版本  
![Image text](https://raw.github.com/cjjloves/homework9/master/problems/版本不兼容2.JPG) 
### 源码
```
import org.apache.spark.{SparkConf, SparkContext}
object SparkSql {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    //val dfs = sqlcontext.read.json("/data/employee.json")
    val dfs = sqlcontext.read.csv("/data/result.txt")
    dfs.show()
    dfs.filter(dfs("_c1") > 5000).show()
    //dfs.filter(dfs(("_c1").split(")")(0).split(" ")(1)) > 1).show()
    //dfs.select("_c1").show()
  }
}
```
## 2. 使用Spark SQL查询统计结果中次数超过k次的词语。
### 源码为SparkSql.scala
&ensp;&ensp;预处理：将键值对(key,value)通过命令行处理替换成(key,value，即删掉最后的“)”，以便适应Spark SQL输入格式  
&ensp;&ensp;k=5000  
![Image text](https://raw.github.com/cjjloves/homework9/master/screenshot/sql-result.JPG)  
