package Week10Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CreateFileinDesktop extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "CreateOutputFile")

  val rdd1 = sc.textFile("src/main/resources/InputFile/Week9Spark/customerorders.csv")

  val rdd2 = rdd1.map(x => (x.split(",")(0),  x.split(",")(2).toFloat))

  val rdd3 = rdd2.reduceByKey((x,y) => x+y)

  val rdd4 = rdd3.sortBy(x => x._2, false)

  val rdd5 =  rdd4.filter( x => x._2 > 5000)

  val rdd6 = rdd5.map (x => (x._1, x._2 *2))

  rdd6.saveAsTextFile("src/main/resources/OutputFile/Week9Spark/spark_output3")

}
