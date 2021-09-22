package Week10Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object GroupByKeyEx extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "GroupByKeyEx")

  val rdd1 = sc.textFile("src/main/resources/InputFile/Week10Spark/bigLog.txt")
  //rdd1.collect.foreach(println)
  val rdd2 = rdd1.map(x => {
    val fields = x.split(":")
    (fields(0),1)
  })
 // rdd2.collect.foreach(println)
 // rdd2.groupByKey.collect().foreach(x => println(x._1, x._2.size))
  rdd2.reduceByKey(_+_).collect().foreach(println)

}
