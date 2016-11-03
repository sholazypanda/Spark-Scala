/**
  * Created by shobhikapanda on 10/22/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.sql.Date
import java.text.SimpleDateFormat
object Question4 {
  def main(args: Array[String]) = {
    val logFile = "src/main/soc-LiveJournal1Adj.txt"
    val dataFile = "src/main/userdata.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val todayDate = new Date(System.currentTimeMillis)
    def ageFriend(dob: String) ={
      val date=dob.split("/")
      val currentMonth = todayDate.getMonth()+1
      val currentYear = todayDate.getYear()+1900
      var age = currentYear - date(2).toInt
      if(date(0).toInt>currentMonth)
      {
        age-=1
      }
      else if(date(0).toInt==currentMonth){
        val currentDay=todayDate.getDate();
        if(date(1).toInt>currentDay)
        {
          age-=1
        }
      }
      age.toFloat
    }
    val logData = sc.textFile(logFile).cache()
    val userData = sc.textFile(dataFile).cache()
    val friendData = logData.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>Array((z,x._1))))
    //friendData.foreach(println)
    val userAgeCalc = userData.map(line=>line.split(",")).map(line=>(line(0),ageFriend(line(9))))
    val joinedDetails=friendData.join(userAgeCalc)
    def maxAgeFriend(ages: Iterable[Float]) = ages.max
    val maxAge = joinedDetails.groupBy(_._2._1).mapValues(ages=>maxAgeFriend(ages.map(_._2._2))).toArray
    val sortedMaxAgeInDesc = maxAge.sortBy(_._2).reverse
    val top10 = sortedMaxAgeInDesc.take(10)
    val top10RDD = sc.parallelize(top10)
    val userAddr = userData.map(line=>line.split(",")).map(line=>(line(0),line(1),line(3),line(4),line(5)))
    val userAddressJoin = userAddr.map({case(userId,lastname,firstname,street,city) => userId->(lastname,firstname,street,city)})
    val result = top10RDD.join(userAddressJoin)
    val ans = result.map(x=>(x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4,x._2._1))
    ans.foreach(println)
    //ans.saveAsTextFile("src/main/output4")
  }
}