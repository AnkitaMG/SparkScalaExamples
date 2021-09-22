package Week9Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Practical4 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","customerorders")

  val rdd1 = sc.textFile("src/main/resources/InputFile/Week9Spark/customerorders.csv")

  val rdd2 = rdd1.map(x=> (x.split(",")(0), x.split(",")(2).toFloat))

  val rdd3 = rdd2.reduceByKey((x,y) => x + y)

  val rdd4 = rdd3.sortBy(x => x._2, false)

  rdd4.collect.foreach(println)

  //rdd1.collect.foreach(println)
}
