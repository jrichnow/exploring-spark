package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File

object OpenRtbRequestLoggerForSlotsAnalyser {

  val slotList = List(108372, 108373, 108374, 108375, 108377, 108365, 108366, 108367, 108368, 108369, 108324, 108325, 108326, 108327, 108328, 108331, 108332, 108333, 108334, 108335)

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-470"
  val requestFileNameHttp = s"$investigationRootFolder/logs/http-bidrequest*.log.gz"
  val requestFileNameOpenRtb = s"$investigationRootFolder/logs/new-openrtb-bidrequest-434*.log.gz"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    //    val rawRequestRDD = sparkContext.textFile(requestFileNameHttp, 2)
    val rawRequestRDD = sparkContext.textFile(requestFileNameOpenRtb, 2)
    println(s"request raw count: ${rawRequestRDD.count}")

    for (sid <- slotList) {
      //      val searchterm = s"sid=$sid"
      val searchterm = "\"tagid\":\"" + sid + "\""
      println(searchterm)
      val filteredRDD = rawRequestRDD.filter(_.contains(searchterm))
      println(s"filter for $sid resulted in count of ${filteredRDD.count}")
      printResult(sid, filteredRDD)
    }
  }

  private def printResult(sid: Int, filteredRDD: RDD[String]) {
    //    val fileName = s"${investigationRootFolder}/http/${sid}-http-requests.txt"
    val fileName = s"${investigationRootFolder}/openrtb/${sid}-openrtb-requests.txt"

    val writer = new PrintWriter(new File(fileName))
    filteredRDD.toArray.foreach(entry => writer.println(entry))
    writer.flush()
    writer.close()
  }

  private def containsSlotId(line: String, jsonSlotList: List[String]): Boolean = {
    for (x <- jsonSlotList) {
      if (line.contains(x)) return true
    }
    false
  }
}