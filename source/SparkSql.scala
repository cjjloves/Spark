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
