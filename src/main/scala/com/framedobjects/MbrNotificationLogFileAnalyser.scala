package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object MbrNotificationLogFileAnalyser {

  val fileName = "/tmp/s3/opt_notif-402--2015-11-04--0.log.gz"
  val iids = Source.fromFile("/tmp/s3/iids.txt").getLines().toSeq

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("MBR Notification Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    

    val rawRDD = sparkContext.textFile(fileName)
    val filteredRDD = rawRDD.filter(filterByImpressionId(_));
    filteredRDD.foreach(println)
  }
  
  def processFile(fileName: String, sc: SparkContext) {
    
  }

  private def filterByImpressionId(logLine: String): Boolean = {
    for (iid <- iids) {
      if (logLine.contains(iid)) return true
    }
    return false
  }
}