/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/memex/tandon/spark-0.9.0-incubating/apps/data.txt" // Should be some file on your system
    val sc = new SparkContext("spark://c8-0-1.local:7077", "Simple App", "/memex/tandon/spark-0.9.0-incubating",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    val logData = sc.textFile(logFile, 1).cache()
    val numAsRDD = logData.filter(line => line.contains("a"))
    val numAs = numAsRDD.count()
    println("Lines with a: %s".format(numAs))
  }
}
