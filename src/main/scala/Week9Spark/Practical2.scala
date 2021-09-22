package Week9Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Practical2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  //val spark = SparkSession.builder.appName(name = "WordCount").config("spark.master","local").getOrCreate()
  val sc = new SparkContext("local[*]","wordcount")

  val rdd1 = sc.textFile("src/main/resources/Week9Spark/search_data.txt")

  val rdd2 = rdd1.flatMap(x => x.split(" "))

  val rdd3 = rdd2.map(x => (x,1))

  val rdd4 =rdd3.reduceByKey((x,y) => x + y)

  val rdd5 = rdd4.sortBy(x=> x._2,false)

  rdd5.collect.foreach(println)

}
