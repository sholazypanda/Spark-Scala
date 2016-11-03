/**
  * Created by shobhikapanda on 10/22/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Question2 {
  def main(args: Array[String]) = {
    val A = readLine("Enter first User : ")
    val B = readLine("Enter second User : ")
    val logFile = "src/main/soc-LiveJournal1Adj.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).cache()
    val friends = logData.map(a => a.split("\\t")).filter(a => (A == a(0) || B == a(0))).filter(a => (a.size == 2)).map(a => a(1).split(","))
    val commonFriend = friends.take(friends.count().toInt).last.intersect(friends.first)
    val result = A + "," + B + "\t" + commonFriend.mkString(",")
    result.foreach(print)
  }
}