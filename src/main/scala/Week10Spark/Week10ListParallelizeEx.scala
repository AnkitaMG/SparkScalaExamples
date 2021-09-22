package Week10Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Week10ListParallelizeEx extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "listParallelize")

  val myList = List("ERROR: Thu Jun 04 BST 2015",
          "WARN: Sun Nov 06 GMT 2016",
          "WARN: Mon Aug 29 1BST 2016",
          "ERROR: Thu Dec 10 1GMT 2015",
          "ERROR: Fri Dec 26 GMT 2014",
          "ERROR: Thu Feb 02 GMT 2017")

    val rdd1 = sc.parallelize(myList)

    val rdd2 = rdd1.map(x => {
      val columns = x.split(":")
      val logLevel = columns(0)
      (logLevel,1)
    }
    )

    val rdd3 = rdd2.reduceByKey((x,y) => x+y)

    rdd3.collect.foreach(println)
}
