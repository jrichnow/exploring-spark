package com.framedobjects.log

import com.framedobjects.model.{ShowHandlerLog, SspBidResponse, AdscaleNotification, AdscaleResponseLogEntry}
import org.apache.spark.{SparkContext, SparkConf}

object CriteoDHLogger {

  def main (args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1559"
    val dspResponseFile = s"$investigationRootFolder/logs/2016-01-06/dsp-adscale-bidresponse-301.log"
    val dspNotificationFile = s"$investigationRootFolder/logs/2016-01-06/dsp-adscale-notification-301.log"
    val sspResponseFile = s"$investigationRootFolder/logs/2016-01-06/ssp-openrtb-bidresponse-30*.log"
    val showHandlerFile = s"$investigationRootFolder/logs/2016-01-06/showhandler-30*.log"

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Criteo Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    // Notifications.
    println("processing dsp notification ...")
    val dspFilteredNotificationRDD = sparkContext.textFile(dspNotificationFile).filter(filterByPartnerId(_)).filter(_.contains("win=true"))
    val dspWinNotificationKeyedByIidRDD = dspFilteredNotificationRDD.map(keyNotification(_))

    // DSP Bid Responses.
    println("processing dsp responses ...")
    val dspRawResponseRDD = sparkContext.textFile(dspResponseFile)
    val dspFilteredResponseRDD = dspRawResponseRDD.filter(filterByPartnerId(_))
    val dspBidsKeyedByIidResponseRDD = dspFilteredResponseRDD.map(keyResponse(_)).filter(_._2 > 0)

    val dspCombinedKeyedByIidRDD = dspWinNotificationKeyedByIidRDD.join(dspBidsKeyedByIidResponseRDD)
    val dspSamePriceRDD = dspCombinedKeyedByIidRDD.filter(filterSamePrice(_))

    // Mapping to get a new combined view.
    val dspMappedSamePriceRDD = dspSamePriceRDD.map(mapNotificationAndResponseBids(_))
    dspMappedSamePriceRDD.foreach(println)

    // SSP Bid Responses.
    val iidArray = dspSamePriceRDD.keys.map(_.substring(0,18)).toArray
    println("processing ssp responses ...")
    val sspResponseRDD = sparkContext.textFile(sspResponseFile)
    val sspFilteredResponseRDD = sspResponseRDD.filter(filterIid(_, iidArray)).map(keySspBidResponse(_))

    val threeBidsKeyedByIidRDD = dspMappedSamePriceRDD.join(sspFilteredResponseRDD)

    // Show Handler.
    val showRDD = sparkContext.textFile(showHandlerFile).filter(filterIid(_, iidArray)).map(keyShowHandlerLog(_))
    val fourRDD = threeBidsKeyedByIidRDD.join(showRDD)
    fourRDD.foreach(println)
  }

  private def filterByPartnerId(line: String): Boolean = {
    line.split(", ")(1).equals("40")
  }

  private def keyResponse(line: String): (String, Double) = {
    val lineArray = line.split(", ")
    (lineArray(0), AdscaleResponseLogEntry.fromJson(lineArray(3)).bid.cpm)
  }

  private def keyNotification(line: String): (String, Double) = {
    val lineArray = line.split(", ")
    (lineArray(0), AdscaleNotification.fromLog(lineArray(2)).price)
  }

  private def filterSamePrice(entry: (String, (Double, Double))): Boolean = {
    val (winNotificationPrice, bidPrice) = entry._2
    winNotificationPrice == bidPrice
  }

  private def filterIid(line: String, iidArray: Array[String]): Boolean = {
    for (iid <- iidArray) {
      if (line.contains(iid)) return true
    }
    false
  }

  private def mapNotificationAndResponseBids(entry: (String, (Double, Double))): (String, String) = {
    val (winNotificationPrice, bidPrice) = entry._2
    (entry._1.substring(0,18), s"dsp-bid:$bidPrice; dsp-win-notif:$winNotificationPrice")
  }

  private def keySspBidResponse(line: String): (String, String) = {
    val response = SspBidResponse.fromJson(line)
    val bid = response.response.seatbid.head.bid.head
    (bid.id, s"ssp-bid:${bid.price}, adid:${bid.adid}")
  }

  private def keyShowHandlerLog(line:String): (String, String) = {
    val showHandlerLog = ShowHandlerLog.fromLog(line)
    (showHandlerLog.iid, s"ssp-win:${showHandlerLog.cost}, slotId:${showHandlerLog.slotId}, adid:${showHandlerLog.advertId}")
  }

}