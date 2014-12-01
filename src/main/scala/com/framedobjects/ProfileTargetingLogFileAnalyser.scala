package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.framedobjects.model.ProfileTargeting
import java.net.URLDecoder
import java.io.PrintWriter
import java.io.File

object ProfileTargetingLogFileAnalyser {

  type CarBrandsByUserKey = (String, String)
  type CarBrands = (String, String)

  //  val logFile = "/users/jensr/development/exploring-spark/src/main/resources/profile_targeting_map.log"
  //  val logFile = "/users/jensr/Documents/DevNotes/Profile Targeting/logs/profile_targeting_map.log"
  val logFile = "/users/jensr/Documents/DevNotes/Profile Targeting/logs/profile_targeting_map*"
  val resultFile = "/users/jensr/Documents/DevNotes/Profile Targeting/logs/pt-result.txt"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("PT Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val logFileRDD = sparkContext.textFile(logFile)

    val ptRDD = logFileRDD.map(line => line.split(": ")(1)).map(ProfileTargeting.fromJson(_))

    val userPtRDD = ptRDD.map(filter)

    val finalUserPtRDD = userPtRDD.reduceByKey((x, y) => s"$x;$y")
    finalUserPtRDD.foreach(println)

    val carBrandRDD = finalUserPtRDD.map(filterCarBrands).distinct
    carBrandRDD.foreach(println)

    val finalCarBrandRDD = carBrandRDD.reduceByKey((x, y) => s"$x, $y")
    finalCarBrandRDD.foreach(println)

    val writer = new PrintWriter(new File(resultFile))
    for ((key, value) <- finalCarBrandRDD.toArray) {
      writer.write(s"$key: $value\n")
    }
    writer.flush
    writer.close

    sparkContext.stop
  }

  private def filterCarBrands(entry: CarBrandsByUserKey): CarBrands = {
    val entryArray = entry._2.split(";")
    entryArray(0) match {
      case a if a.startsWith("ads20") => {
        if (entryArray.length > 1)
          (a.split("=")(1), entryArray(1).split("=")(1))
        else
          (a.split("=")(1), "")
      }
      case b if b.startsWith("ads21") => {
        if (entryArray.length > 1)
          (entryArray(1).split("=")(1), b.split("=")(1))
        else
          ("missing car make", b.split("=")(1))
      }
      case _ => ("key", entryArray(0))
    }
  }

  private def filter(pt: ProfileTargeting): CarBrandsByUserKey = {
    val value = URLDecoder.decode(pt.value)
    (s"${pt.uu}-${pt.publisherId}-${pt.slotId}", s"${pt.key}=$value")
  }
}