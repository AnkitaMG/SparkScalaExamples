package Week10Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object Week10BroadcastVariable extends App {

  def loadBoringWords(): Set[String] ={
    var boringWords:Set[String] =Set()
    val lines = Source.fromFile("C:/Users/AnkitaM/Documents/BIGDATA/Week10Spark/boringwords.txt").getLines()

    for (line <- lines) {
      boringWords += line
    }
    boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "BroadcastVariable")

  var nameSet = sc.broadcast(loadBoringWords)

  val rdd1 = sc.textFile("C:/Users/AnkitaM/Documents/BIGDATA/Week10Spark/bigdatacampaigndata.csv")

  val rdd2 = rdd1.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))

  val rdd3 = rdd2.flatMapValues(x => x.split(" "))

  val rdd4 = rdd3.map(x => (x._2.toLowerCase(), x._1))

  val rdd10 = rdd4.filter(x => !nameSet.value(x._1))

  val rdd11 = rdd10.reduceByKey((x, y) => x + y)

  val rdd6 = rdd11.sortBy(x => x._2,false)
  rdd6.collect.foreach(println)
}
