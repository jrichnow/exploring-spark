package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.framedobjects.model.ProfileTargeting

object ProfileTargetingLogFileAnalyser {

  type CarBrandsByUserKey = (String, String)
  type CarBrands = (String, String)

  val logFile = "/users/jensr/development/exploring-spark/src/main/resources/profile_targeting_map.log"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("PT Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val logFileRDD = sparkContext.textFile(logFile, 2)

    val ptRDD = logFileRDD.map(line => line.split(": ")(1)).map(ProfileTargeting.fromJson(_))

    val userPtRDD = ptRDD.map(filter(_))

    val finalUserPtRDD = userPtRDD.reduceByKey((x,y) => s"$x;$y")
    finalUserPtRDD.foreach(println)

    val carBrandRDD = finalUserPtRDD.map(filterCarBrands)
    carBrandRDD.foreach(println)

    val finalCarBrandRDD = carBrandRDD.reduceByKey(reduceBrands)
    finalCarBrandRDD.foreach(println)

    sparkContext.stop
  }

  private def reduceBrands(x: String, y: String): String = {
    if (y.contains(x))
      y
    else
      s"$x,$y"
  }

  private def filterCarBrands(entry: CarBrandsByUserKey): CarBrands = {
    val entryArray = entry._2.split(";")
    entryArray(0) match {
      case a if a.startsWith("ads20") => {
        (a.split("=")(1), entryArray(1).split("=")(1))
      }
      case b if b.startsWith("ads21") => {
        (entryArray(1).split("=")(1), b.split("=")(1))
      }
      case _ => ("Wrong", "Wrong")
    }
  }

  private def filter(pt: ProfileTargeting): CarBrandsByUserKey = {
    (s"${pt.uu}-${pt.publisherId}-${pt.slotId}", s"${pt.key}=${pt.value}")
  }
}