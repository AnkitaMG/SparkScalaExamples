package Week9Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Practical6 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "averageconnections")

  val inputfile = sc.textFile("src/main/resources/InputFile/Week9Spark/friendsdata.csv")

  val requireddata = inputfile.map (x => (x.split("::")(2).toInt,x.split("::")(3).toInt))

  val tupledata = requireddata.map(x => (x._1,(x._2,1)))

  val totalByAge =tupledata.reduceByKey( (x,y) => (x._1+y._1, x._2 + y._2))

  //val average = totalByAge.map (x => (x._1, x._2._1/ x._2._2 )).sortBy(x => x._2, false)

  val average = totalByAge.mapValues(x => x._1/x._2).sortBy(x => x._2, false) //alternate method using mapValues

  average.collect.foreach(println)
}


