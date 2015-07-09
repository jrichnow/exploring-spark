package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.jackson.Json
import com.framedobjects.model.OpenRtbRequestLogEntry
import com.framedobjects.model.OpenRtbResponseLogEntry
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File

object MbrOpenRtbAnalyser {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-538a"
  val requestFileName = s"$investigationRootFolder/logs/new-openrtb-bidrequest-422.log"
  val responseFileName = s"$investigationRootFolder/logs/new-openrtb-bidresponse-422.log"
//  val notificationFileName = s"$investigationRootFolder/logs/rtb-notifications-422.log"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
    val rawRequestRDD = sparkContext.textFile(requestFileName, 2)
    val filteredRawRequestRDD = rawRequestRDD.filter(_.contains("\"tpid\":48")).filter(_.contains("pmp"))
    val jsonRequestRDD = filteredRawRequestRDD.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id)
    val requestIds = jsonRequestRDD.toArray
    
    val rawResponseRDD = sparkContext.textFile(responseFileName, 2)
    val filteredResponseRDD = rawResponseRDD.filter(filterByRequestId(_, requestIds))
    
//    val rawNotificationRDD = sparkContext.textFile(notificationFileName, 2)
//    val filteredNotificationRDD = rawNotificationRDD.filter(filterByRequestId(_, requestIds))
    
    println(s"requests: ${jsonRequestRDD.count}")
    println(s"responses: ${filteredResponseRDD.count}")
//    println(s"notifications: ${filteredNotificationRDD.count}")
    
    writeResultFile(s"$investigationRootFolder/requests.log", filteredRawRequestRDD)
    writeResultFile(s"$investigationRootFolder/responses.log", filteredResponseRDD)
//    writeResultFile(s"$investigationRootFolder/notifications.log", filteredNotificationRDD)
  }
  
  private def writeResultFile(fileName: String, rdd: RDD[String]) {
    val writer = new PrintWriter(new File(fileName))
    rdd.toArray.foreach(line => writer.println(line))
    writer.flush()
    writer.close()
  }
  
  private def filterByRequestId(line: String, requestIds: Array[String]): Boolean = {
    for (requestId <- requestIds) {
      if (line.contains(requestId)) {
        return true
      }
    }
    false
  }
}