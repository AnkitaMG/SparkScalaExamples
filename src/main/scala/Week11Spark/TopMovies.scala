package Week11Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TopMovies extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "CreateOutputFile")

  val rdd1 = sc.textFile("src/main/resources/InputFile/Week11Spark/ratings.dat")

  val rdd2 = rdd1.map (x => (x.split("::")(1), x.split("::")(2)))

  val rdd3 = rdd2.map(x => (x._1,(x._2.toFloat,1.0)))

  val rdd4 = rdd3.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val rdd5 = rdd4.filter( x => x._2._2 >1000)

  val rdd6 = rdd5.mapValues(x => (x._1/x._2)).filter( x=> x._2 >4.5)

  val rdd7 = sc.textFile("src/main/resources/InputFile/Week11Spark/movies.dat")

  val rdd8 = rdd7.map (x=> (x.split("::")(0), x.split("::")(1)))

  val rdd9 = rdd8.join(rdd6)

  val rdd10 = rdd9.map (x=> x._2._1)

  rdd10.collect.foreach(println)

}
