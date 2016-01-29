package com.framedobjects.log

import java.io.{File, PrintWriter}

import com.framedobjects.model.{OpenRtbNotificationLogEntry, OpenRtbRequestLogEntry, OpenRtbResponseLogEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object InviteBiddingLogging {

  val dealId = "agizyyq6ae"
  val partnerString = "\"tpid\":38"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1674"
    val investigationOutFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1674"
    val deal: String = s"$dealId"
    runJob(sparkContext, investigationRootFolder, investigationOutFolder, deal)
  }


  def runJob(sparkContext: SparkContext, investigationRootFolder: String, investigationOutFolder: String, deal: String): Unit = {
    val bidRequestFileName = s"$investigationRootFolder/$deal.log.gz"
    val bidresponseFileName = s"$investigationRootFolder/dsp-openrtb-bidresponse-418--2016-01-27--*.log.gz"
    val notificationFileName = s"$investigationRootFolder/dsp-openrtb-notification-418--2016-01-27--*.log.gz"
    val resultFileName = s"$investigationOutFolder/$deal.csv"

    val rawBidRequestRDD = sparkContext.textFile(bidRequestFileName)
    val filterByDealRequestRDD = rawBidRequestRDD.map(mapDealInRequestByIid(_))
    val filteredRawBidResponseRDD = sparkContext.textFile(bidresponseFileName).filter(_.contains(partnerString)).map(mapResponseByRequestId(_))
    val joinedRDD = filterByDealRequestRDD.join(filteredRawBidResponseRDD)
    val filteredNotificationRDD = sparkContext.textFile(notificationFileName).filter(_.contains(partnerString)).map(mapNotificationByRequestId(_))
    val allJoinedRDD = joinedRDD.leftOuterJoin(filteredNotificationRDD)
    val csv = allJoinedRDD.map { case (requestId, ((request, response), notification)) =>
      s"$requestId,${requestId.substring(0, 18)},$request,$response,${notification.getOrElse(",,")}"
    }
    csv.repartition(1).saveAsTextFile(resultFileName)
  }


  private def mapDealInRequestByIid(line: String): (String, String) = {
    val entry = OpenRtbRequestLogEntry.fromJson(line)
    val impr = entry.request.imp.head
    val site = entry.request.site
    (entry.request.id,  s"${site.publisher.id},${site.id},${impr.tagid},${escapeUrlsWithComma(site.domain)},${{escapeUrlsWithComma(site.page)}},${{escapeUrlsWithComma(site.ref)}}")
  }

  private def escapeUrlsWithComma(url: Option[String]): String = {
    val escapedUrl = url match {
      case a: Some[String] if a.get.contains(",") => "\"" + a.get + "\""
      case b: Some[String] => b.get
      case None => ""
    }
    escapedUrl
  }

  private def mapResponseByRequestId(line: String): (String, String) = {
    val response = OpenRtbResponseLogEntry.fromJson(line)
    val seat = response.response.seatbid

    // Get bid price
    val bidPrice = seat match {
      case None => 0
      case sBid if sBid.get.length > 0 => sBid.get.head.bid.head.price
      case _ => 0
    }
    // Get bid type
    val bidType = line match {
      case noBid if noBid.contains("http-status") => "NB"
      case deal if deal.contains(dealId) => "PD"
      case _ => "OA"
    }
    // Get adomain.
    val adomain = seat match {
      case None => ""
      case sBid if sBid.get.length > 0 => sBid.get.head.bid.head.adomain.head
      case _ => ""
    }
    // Get avn.
    val avn = seat match {
      case None => ""
      case sBid if sBid.get.length > 0 => sBid.get.head.bid.head.ext.get.avn
      case _ => ""
    }
    // Get crid.
    val crid = seat match {
      case None => ""
      case sBid if sBid.get.length > 0 => sBid.get.head.bid.head.crid
      case _ => ""
    }


    (response.requestId, s"$bidType,$bidPrice,$adomain,$avn,$crid")
  }

  private def mapNotificationByRequestId(line: String): (String, String) = {
    val notifLog = OpenRtbNotificationLogEntry.fromJson(line)
    val notif = notifLog.notifications.notifications.head
    (notif.id, s"${notif.win},${notif.p},${notif.r.getOrElse("")}")
  }

  private def saveResult(resultRDD: RDD[(String, ((String, String), Option[String]))], fileName: String): Unit = {
    val writer = new PrintWriter(new File(fileName))
    writer.println("requestId,impressionId,publisherId,websiteID, slotId,domain,pageUrl,referrer,bidType,bid,adomain,avn,crid,win,price,loseReason")
    val resultArray = resultRDD.collect()
    resultArray.foreach{
      case (requestId, ((request, response), notification)) => {
        val resultLine = s"$requestId,${requestId.substring(0,18)},$request,$response,${notification.getOrElse(",,")}"
        writer.println(resultLine)
      }
    }
    writer.flush()
    writer.close()
  }
}