/**
  * Created by shobhikapanda on 10/29/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object MLProj{
def main(args: Array[String]) = {

  val logFile = "/Users/shobhikapanda/Downloads/ml dataset kaggle/train_numeric.csv"
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val logData = sc.textFile(logFile).cache()
  logData.take(100).foreach(println)
  print(logData.count())

}
}
