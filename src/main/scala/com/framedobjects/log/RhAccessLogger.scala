package com.framedobjects.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.RhAccessLog

object RhAccessLogger {

  val rhPixelId = "rh.adscale.de/rh/15174/PU_ZD_test_mpnm"

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1182"
  val logFileName = s"$investigationRootFolder/logs/rh-access-logs/rh-access-app11.2015-08-22-00.log.gz"

  def main(args: Array[String]) {

    val sparkConfig = new SparkConf().setMaster("local").setAppName("IhAccessLog Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val rhAccessLogRDD = sparkContext.textFile(logFileName).map(RhAccessLog.fromLog(_))
    println(rhAccessLogRDD.count())

    val filteredRDD = rhAccessLogRDD.filter(_.impressionId.startsWith("401")).filter(_.pixelId.contains(rhPixelId))
    println(filteredRDD.count())
  }
}