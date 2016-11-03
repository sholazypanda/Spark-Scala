import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Map => MMap}
object SparkWordCount {
  def main(args: Array[String]) {
    val logFile = "src/main/soc-LiveJournal1Adj.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).cache()
    var mapOperation = logData.map(x=>x.split("\t")).filter(x => x.length == 2).map(x => (x(0),x(1).split(",")))
    var map = scala.collection.mutable.Map[String,List[String]]()
    var secondMap = scala.collection.mutable.Map[String,List[String]]()
    var changedMap = mapOperation.map{
      line =>
        var key ="";
        for(userIds <- line._2){
            if(line._1.toInt < userIds.toInt){
              key = line._1.concat("-"+userIds);
            }
            else {
              key = userIds.concat("-"+line._1);
            }
          if(!map.contains(key))
            map = map + (key -> line._2.toList)
          else
            secondMap = secondMap + (key -> line._2.toList)
        }
    }
    changedMap.take(200).foreach(println)

    var mapRDD = sc.parallelize(map.toList)
    var secondMapRDD = sc.parallelize(secondMap.toList)
    val joinedRDD = mapRDD.join(secondMapRDD)
    val finalResult = joinedRDD.map(tuple => {
      val matchedLists = tuple._2
      val intersectValues = matchedLists._1.intersect(matchedLists._2)
      (tuple._1, intersectValues)
    })

     var arr = finalResult.saveAsTextFile("src/main/scala/output.txt")
  }
}