package Week10Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Week10RealTimeExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "realtimeexample")

  val inputFile = sc.textFile("C:/Users/AnkitaM/Documents/BIGDATA/Week10Spark/bigdatacampaigndata.csv")

  val requireData = inputFile.map(x => (x.split(",")(10).toFloat,  x.split(",")(0)))

  val rdd1 = requireData.flatMapValues(x => x.split(" "))

  val rdd2 = rdd1.map(x => ( x._2.toLowerCase(), x._1))
  val rdd3 = rdd2.reduceByKey((x, y) => x+y)
  val rdd4 = rdd3.sortBy(x => x._2,false)
  rdd4.collect.foreach(println)



}
