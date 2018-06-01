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
