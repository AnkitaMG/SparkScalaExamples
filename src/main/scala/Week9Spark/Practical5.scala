package Week9Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Practical5 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "movieratings")

  val inputfile = sc.textFile("src/main/resources/InputFile/Week9Spark/moviedata.data")

  val ratinglist = inputfile.map(x =>x.split("\t")(2))
  //println("Manral printing rating list")
  //ratinglist.collect.foreach(println)
 // val ratingtuple = ratinglist.map( x => (x,1))
  //println("Manral printing rating tuple")
  //ratingtuple.collect.foreach(println)
  //val ratingcount = ratingtuple.reduceByKey((x ,y) => x+y)
  //println("Manral printing rating count")
 // ratingcount.collect.foreach(println)

  val totalratingcount = ratinglist.countByValue().foreach(println) //This is alternate way directly using countByValue Action

}
