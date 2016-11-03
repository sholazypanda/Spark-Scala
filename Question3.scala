/**
  * Created by shobhikapanda on 10/22/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Question3 {
  def main(args: Array[String]) = {
    val A = readLine("Enter first User : ")
    val B = readLine("Enter second User : ")
    val logFile = "src/main/soc-LiveJournal1Adj.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).cache()
    val friends = logData.map(a=>a.split("\\t")).filter(a=>(A==a(0) || B==a(0))).filter(a => (a.size == 2)).map(a=>a(1).split(","))
    val commonFriend=friends.take(friends.count().toInt).last.intersect(friends.first)
    val userData = sc.textFile("src/main/userdata.txt")
    val frienddetails = userData.map(a=>a.split(",")).map(a=>(a(0),(a(1),a(6),a(9))))
    val commonFrd=sc.parallelize(commonFriend)
    val key=commonFrd.map(y=>y->y)
    val res=key.join(frienddetails)
    val answer=A+" "+B+"\t"+"("+res.map(x=>(x._2._2._1+":"+x._2._2._2+":"+x._2._2._3)).collect().mkString(",")+")"
    answer.foreach(print)
  }
}