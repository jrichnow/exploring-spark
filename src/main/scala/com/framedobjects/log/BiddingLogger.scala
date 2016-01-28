package com.framedobjects.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.AdscaleResponseLogEntry
import scala.util.Try
import com.framedobjects.model.OpenRtbResponseLogEntry
import com.framedobjects.model.AppnexusResponseLogEntry
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import com.framedobjects.model.AdscaleNotification

object BiddingLogger {

  val date = "2015-09-02"
  val rootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1213"
  val adscaleRequestFile = s"$rootFolder/logs/$date/dsp-adscale-bidrequest-401--$date--*.log.gz"
  val adscaleResponseFile = s"$rootFolder/logs/$date/dsp-adscale-bidresponse-401--$date--*.log.gz"
  val adscaleNotificationFile = s"$rootFolder/logs/$date/dsp-adscale-notification-401.log"
  val openRtbResponseFile = s"$rootFolder/logs/$date/dsp-openrtb-bidresponse-401--$date--*.log.gz"
  val appnexusResponseFile = s"$rootFolder/logs/$date/dsp-appnexus-bidresponse-401.log"

  val criteoId = "40"

  def mainNotif(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Bidding behaviour investigation")
    val sparkContext = new SparkContext(sparkConfig)
    val notificationRDD = sparkContext.textFile(adscaleNotificationFile, 2).filter(_.contains(", 40, ")).map(keyAdscaleNotification(_))
    notificationRDD.top(10).foreach(println)
    println(s"${notificationRDD.count()} criteo notifications")

  }

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Bidding behaviour investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val rawAdscaleRequestsRDD = sparkContext.textFile(adscaleRequestFile, 2).filter(filterAdscaleRequestByPartnerId(criteoId, _))
    println(s"${rawAdscaleRequestsRDD.count()} criteo bid requests")

    // Get impression IDs for requests that are sent to Criteo
    val requestIids = rawAdscaleRequestsRDD.map(mapAdscaleRequestToImpressionId(_)).toArray()
    println(s"${requestIids.length} impression IDs in request to criteo")

    val rawAdscaleResponseRDD = sparkContext.textFile(adscaleResponseFile, 2).persist(StorageLevel.MEMORY_AND_DISK)

    // Get impression IDs for which criteo was bidding > 0
    val criteoBidsIidArray = rawAdscaleResponseRDD
      .filter(filterContainsString(_, requestIids))
      .filter(filterCriteoNoneZeroBids(_))
      .map(_.substring(0, 20)).toArray()
    println(s"${criteoBidsIidArray.length} impression IDs in response for criteo bids > 0")

    // Get all adscale responses for criteo bids > 0.
    val keyedAdscaleBidsRDD = rawAdscaleResponseRDD
      .filter(filterContainsString(_, criteoBidsIidArray))
      .map(keyAdscaleResponse(_))
    println(s"${keyedAdscaleBidsRDD.count()} adscale bid responses")

    // Get openRTB bids > 0 for these criteo bids > 0
    val keyedOpenRtbBidsRDD = sparkContext.textFile(openRtbResponseFile, 2)
      .filter(filterContainsString(_, criteoBidsIidArray))
      .filter(!_.contains("\"http-status\":204"))
      .filter(!_.contains("\"tpid\":58")) // and not 58 as it has pretty printing on.
      .map(keyOpenRtbResponse(_))
    println(s"${keyedOpenRtbBidsRDD.count()} openRtb bids > 0")

    // Get appnexus responses > 0 for criteo bids > 0
    val keyedAppnexusBidsRDD = sparkContext.textFile(appnexusResponseFile, 2)
      .filter(filterContainsString(_, criteoBidsIidArray))
      .filter(_.contains("\"no_bid\":false"))
      .map(keyAppnexusBids(_))
    println(s"${keyedAppnexusBidsRDD.count()} appnexus bids > 0")

    // Now get all bid requests together
    val allKeyedBidsRDD = keyedAdscaleBidsRDD.union(keyedOpenRtbBidsRDD).union(keyedAppnexusBidsRDD).groupByKey()
    allKeyedBidsRDD.top(10).foreach(println)
    println(s"${allKeyedBidsRDD.count()} all bids")

    // Now sort the bids by price.
    val sortedRDD = allKeyedBidsRDD.map(sortByPrice(_))
    sortedRDD.top(10).foreach(println)

    implicit val highestBidder = new Ordering[List[(String, Double)]] {
      override def compare(l1: List[(String, Double)], l2: List[(String, Double)]): Int = {
        l1.head._2.compareTo(l2.head._2)
      }
    }

    val winningRDD = sortedRDD.filter(filterPartnerIsWinning(criteoId, _))
    winningRDD.top(10).foreach(println)
    println(s"${winningRDD.count()} criteo wins")

    val losingRDD = sortedRDD.filter(!filterPartnerIsWinning(criteoId, _))
    losingRDD.top(10).foreach(println)
    println(s"${losingRDD.count()} criteo loses")

    val notificationRDD = sparkContext.textFile(adscaleNotificationFile, 2).filter(_.contains(", 40, ")).map(keyAdscaleNotification(_))
    notificationRDD.top(10).foreach(println)
    println(s"${notificationRDD.count()} criteo notifications")

    val winAndNotifRDD = winningRDD.join(notificationRDD)
    winAndNotifRDD.top(20).foreach(println)
    println(s"${winAndNotifRDD.count()} criteo wins and notifs")

    val actualWinByNotifRDD = winAndNotifRDD.filter(_._2._2.contains("win=true"))
    actualWinByNotifRDD.top(20).foreach(println)
    println(s"${actualWinByNotifRDD.count()} criteo actual wins per notifs")

    val notWinByNotifRDD = winAndNotifRDD.filter(_._2._2.contains("win=false"))
    notWinByNotifRDD.top(20).foreach(println)
    println(s"${notWinByNotifRDD.count()} criteo no wins per notifs")

    writeResultFile2(s"$rootFolder/criteo-highest-bidder-and-won.txt", winAndNotifRDD)
    writeResultFile2(s"$rootFolder/criteo-highest-bidder-and-lost.txt", notWinByNotifRDD)
    writeResultFile(s"$rootFolder/criteo-not-highest-bidder.txt", losingRDD)
    //    losingRDD.map(a => (a._2, a._1)).sortBy(_._1.toList, false).top(100).foreach(println)
  }

  private def filterAdscaleRequestByPartnerId(partnerId: String, logEntry: String): Boolean = {
    logEntry.split(", ")(1).equals(partnerId)
  }

  private def filterContainsString(line: String, list: Seq[String]): Boolean = {
    for (value <- list) {
      if (line.contains(value)) return true
    }
    false
  }

  private def mapAdscaleRequestToImpressionId(logEntry: String): String = {
    logEntry.split(", ")(0).substring(0, 20)
  }

  private def keyAdscaleResponse(logEntry: String): (String, (String, Double)) = {
    val logParts = logEntry.split(", ")
    val iid = logParts(0).substring(0, 20)
    val partnerId = logParts(1)
    val bid = extractAdscaleBid(logEntry).getOrElse(-1d)
    (iid, (partnerId, bid))
  }

  private def keyOpenRtbResponse(line: String): (String, (String, Double)) = {
    val json = OpenRtbResponseLogEntry.fromJson(line)
    val price = json.response.seatbid.get(0).bid(0).price
    (json.response.id.get.substring(0, 20), (json.tpid.toString(), price))
  }

  private def keyAdscaleNotification(line: String): (String, String) = {
    val e = line.split(", ")
    val notif = AdscaleNotification.fromLog(e(2))
    (e(0).substring(0, 20), s"win=${notif.win}, price=${notif.price}, reason=${notif.r.getOrElse("")}")
  }

  private def extractAdscaleBid(logEntry: String): Try[Double] = {
    val jsonString = logEntry.split(", \\{")(1)
    Try(AdscaleResponseLogEntry.fromJson(s"{$jsonString").bid.cpm)
  }

  private def filterCriteoNoneZeroBids(line: String): Boolean = {
    val logEntries = line.split(", ")
    val isCriteoBid = logEntries(1).equals(criteoId)
    if (isCriteoBid && extractAdscaleBid(line).getOrElse(-1d) > 0)
      true
    else
      false
  }

  private def keyAppnexusBids(line: String): (String, (String, Double)) = {
    val a = line.split(", ")

    (a(0).substring(0, 20), ("38", AppnexusResponseLogEntry.fromJson(a(2)).bid.getOrElse(0)))
  }

  private def sortByPrice(a: (String, Iterable[(String, Double)])): (String, Iterable[(String, Double)]) = {
    val sortedBids = a._2.toList.sortBy(_._2).reverse
    (a._1, sortedBids)
  }

  private def filterPartnerIsWinning(partnerId: String, a: (String, Iterable[(String, Double)])): Boolean = {
    a._2.head._1.equals(partnerId)
  }

  private def writeResultFile(fileName: String, rdd: RDD[(String, Iterable[(String, Double)])]) {
    val writer = new PrintWriter(new File(fileName))
    rdd.toArray.foreach(e => {
      writer.println(s"${e._1.substring(0, 18)} - ${e._2.mkString(",")}")
    })
    writer.flush()
    writer.close()
  }

  private def writeResultFile2(fileName: String, rdd: RDD[(String, (Iterable[(String, Double)], String))]) {
    val writer = new PrintWriter(new File(fileName))
    rdd.toArray.foreach(e => {
      writer.println(s"${e._1.substring(0, 18)} - ${e._2._1.mkString(",")} - ${e._2._2}")
    })
    writer.flush()
    writer.close()
  }
}