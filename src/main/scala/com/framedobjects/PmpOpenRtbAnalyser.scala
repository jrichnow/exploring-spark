package com.framedobjects

import java.io.File
import java.io.PrintWriter
import scala.Array.canBuildFrom
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.framedobjects.model.OpenRtbRequestLogEntry
import com.framedobjects.model.OpenRtbResponseLogEntry
import com.framedobjects.model.AdscaleResponseLogEntry
import com.framedobjects.model.AppnexusResponseLogEntry
import scala.util.Try
import org.joda.time.DateTime
import java.util.Date

object PmpOpenRtbAnalyser {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-981"
    
  // Input files.
//  val requestOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidrequest-434--2015-07-09--0.log.gz"
  val requestOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidrequest-434*"
//  val requestHttpFileName = s"$investigationRootFolder/logs/http-bidrequest-434--2015-07-09--1.log.gz"
  val requestHttpFileName = s"$investigationRootFolder/logs/http-bidrequest-434*"
//  val responseOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidresponse-434--2015-07-09--0.log.gz"
  val responseOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidresponse-434*"
//  val responseHttpFileName = s"$investigationRootFolder/logs/http-bidresponse-434--2015-07-09--0.log.gz"
  val responseHttpFileName = s"$investigationRootFolder/logs/http-bidresponse-434*"
  val notificationOenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-notification-434.log"
  
  def main(args: Array[String]) {
    //  val dealIds = List("nynolvglon", "zxlkic4cnn", "77jgkhc4ca", "dgq2fznukl")
    val dealIds = List("dgq2fznukl")

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    for (dealId <- dealIds) {
      val rawOpenRtbRequestRDD = sparkContext.textFile(requestOpenRtbFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawOpenRtbRequestByDealIdRDD = rawOpenRtbRequestRDD.filter(_.contains(dealId))
      val iids = rawOpenRtbRequestByDealIdRDD.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id).toArray.map(_.substring(0, 20))

      val rawOpenRtbRequestByIidRDD = rawOpenRtbRequestRDD.filter(filterContainsString(_, iids)).map(mapByIidOpenRtbRequest(_))

      rawOpenRtbRequestByDealIdRDD.foreach(println)
      iids.foreach(println)
      rawOpenRtbRequestByIidRDD.foreach(println)

      // Get the entries from the HTTP requests.
      //      val rawHttpRequestRDD = sparkContext.textFile(requestHttpFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      //      val rawHttpRequestByIidRDD = rawHttpRequestRDD.filter(filterContainsString(_, iids)).map(mapByIidHttpRequest(_))
      //      rawHttpRequestByIidRDD.foreach(println)

      //      val combinedRequestRDD = rawOpenRtbRequestByIidRDD.union(rawHttpRequestByIidRDD).groupByKey
      //      combinedRequestRDD.foreach(println)

      // Get the entries from the OpenRtb responses.
      val rawOpenRtbResponseRDD = sparkContext.textFile(responseOpenRtbFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawOpenRtbResponseByIidRDD = rawOpenRtbResponseRDD.filter(filterContainsString(_, iids)).filter(_.contains("\"tpid\":48")).map(mapByIidOpenRtbResponse(_))
      rawOpenRtbResponseByIidRDD.foreach(println)

      // Get the entries from the Http responses.
      val rawHttpResponseRDD = sparkContext.textFile(responseHttpFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawHttpResponseByIidRDD = rawHttpResponseRDD.filter(filterContainsString(_, iids)).filter(filterHttpResponseMbr(_)).map(mapByIidHttpResponse(_))
      rawHttpResponseByIidRDD.foreach(println)

      val combinedResponseRDD = rawOpenRtbResponseByIidRDD.union(rawHttpResponseByIidRDD).groupByKey.persist(StorageLevel.MEMORY_AND_DISK)
      combinedResponseRDD.foreach(println)
      val iidList = combinedResponseRDD.keys.toArray

      writeResultFile(s"$investigationRootFolder/responses-$dealId.log", combinedResponseRDD)
      
      // Get the notifications.
      val rawOpenRtbNotificationsRDD = sparkContext.textFile(notificationOenRtbFileName, 2)
      val filteredNotifications = rawOpenRtbNotificationsRDD.filter(filterContainsString(_, iidList))
      writeNotificationFile(s"$investigationRootFolder/notifications-$dealId.log", filteredNotifications)
    }
  }

  private def writeResultFile(fileName: String, rdd: RDD[(String, Iterable[String])]) {
    val writer = new PrintWriter(new File(fileName))
    rdd.toArray.foreach(e => {
      val date = new DateTime(e._1.substring(5, 18).toLong)
      writer.println(s"$date; ${e._1}; ${e._2.mkString(", ")}")
    })
    writer.flush()
    writer.close()
    
    // Print all iids
    println(rdd.keys.toArray.map(_.substring(0, 18)).mkString(","))
  }
  
  private def writeNotificationFile(fileName: String, rdd: RDD[String]) {
	  val writer = new PrintWriter(new File(fileName))
	  rdd.toArray.foreach(e => writer.println(e))
	  writer.flush()
	  writer.close()
  }

  private def filterContainsString(line: String, list: Seq[String]): Boolean = {
    for (value <- list) {
      if (line.contains(value)) return true
    }
    false
  }

  private def filterHttpResponseMbr(line: String): Boolean = {
    val lineArray = line.split(", ")
    lineArray(1).trim() match {
      case "48" => false
      case _ => true
    }
  }

  private def mapByIidOpenRtbRequest(line: String): (String, String) = {
    (OpenRtbRequestLogEntry.fromJson(line).request.id.substring(0, 20), line)
  }

  private def mapByIidHttpRequest(line: String): (String, String) = {
    (line.split(" ")(1).substring(0, 20), line)
  }

  private def mapByIidOpenRtbResponse(line: String): (String, String) = {
    val json = OpenRtbResponseLogEntry.fromJson(line)
    val price = json.response.seatbid(0).bid(0).price
    (json.response.id.substring(0, 20), s"${json.tpid} -> $price")
  }
  
  private def mapByIidHttpResponse(line: String): (String, String) = {
    val lineArray = line.split(", ")
    val tpid = lineArray(1).trim()
    val bid = tpid match {
      case "45" => if (AppnexusResponseLogEntry.fromJson(lineArray(3).trim()).no_bid) 0 else -1
      case _ => extractAdscaleBid(lineArray(3).trim()).getOrElse(-1)
    }
    (lineArray(0).substring(0, 20), s"$tpid -> $bid")
  }

  private def extractAdscaleBid(jsonString: String): Try[Double] = {
    Try(AdscaleResponseLogEntry.fromJson(jsonString).bid.cpm)
  }

  private def extractIids(a: (String, Iterable[String])): (String, Seq[String]) = {
    println(a._1, a._2.seq.size)
    a._2.foreach(println)
    val iidList = a._2.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id).map(_.substring(0, 20))
    iidList.foreach(println)
    (a._1, iidList.toSeq)
  }
}