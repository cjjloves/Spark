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
&ensp;&ensp;预处理：将键值对(key,value)通过命令行处理替换成(key,value，即删掉最后的“)”，以便适应Spark SQL输入格式  
&ensp;&ensp;k=5000  
![Image text](https://raw.github.com/cjjloves/homework9/master/screenshot/sql-result.JPG)  
### 源码
```
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import org.ansj.splitWord.analysis.DicAnalysis
import org.ansj.library.DicLibrary
import org.ansj.recognition.impl.StopRecognition


object testwordcount {
  def main (args: Array[String]) {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //System.setProperty("spark.ui.showConsoleProgress","False")
    //----添加自定义词典----
    val dicfile = raw"/home/u2/hadoop_installs/hadoop-2.7.4/project1/chi_words.txt" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
    for (word <- Source.fromFile(dicfile).getLines) { DicLibrary.insert(DicLibrary.DEFAULT,word)} //逐行读入文本文件，将其添加到自定义词典中
    println("done")
    //----添加停用词----
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点
  

    // ----构建spark对象----
    val conf = new SparkConf().setAppName("TextClassificationDemo").setMaster("local")
    val sc = new SparkContext(conf)
    //----读入要分词的文件----
    val filename = raw"/home/u2/hadoop_installs/hadoop-2.7.4/fulldata/fulldata.txt"
    val CSVFile = sc.textFile(filename) // 用sc读入文件，此时文件的数据是RDD结构，注意textFile只能读UTF-8编码
    //val splited = CSVFile.map( x => DicAnalysis.parse(x).recognition(filter).toStringWithOutNature("|") )

    //----进行分词----
    val splited = CSVFile.map( x => DicAnalysis.parse(x.split("\t")(4)).recognition(filter).toStringWithOutNature(" ") )
    //splited.foreach(println)

    //----map and reduce
    val words = splited.flatMap(line => line.split(" "))
    val wordPairs = words.map(word => (word, 1))
    val wordCounts = wordPairs.reduceByKey((a, b) => a + b).map(pair=>(pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))
    //wordCounts.collect().foreach(println)
    wordCounts.saveAsTextFile("/data/WC-scala")
  }
}
```
