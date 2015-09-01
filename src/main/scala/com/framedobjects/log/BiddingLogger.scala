package com.framedobjects.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.AdscaleResponseLogEntry
import scala.util.Try
import com.framedobjects.model.OpenRtbResponseLogEntry
import com.framedobjects.model.AppnexusResponseLogEntry
import org.apache.spark.storage.StorageLevel

object BiddingLogger {

  val rootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1213"
  val adscaleRequestFile = s"$rootFolder/logs/dsp-adscale-bidrequest-401--2015-08-31--0.log.gz"
  val adscaleResponseFile = s"$rootFolder/logs/dsp-adscale-bidresponse-401--2015-08-31--0.log.gz"
  val openRtbResponseFile = s"$rootFolder/logs/dsp-openrtb-bidresponse-401--2015-08-31--0.log.gz"
  val appnexusResponseFile = s"$rootFolder/logs/dsp-appnexus-bidresponse-401.log"

  val criteoId = "40"

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
      .map(_.substring(0,20)).toArray()
    println(s"${criteoBidsIidArray.length} impression IDs in response for criteo bids > 0")

    // Get all adscale responses for criteo bids > 0.
    val keyedAdscaleBidsRDD = rawAdscaleResponseRDD
      .filter(filterContainsString(_, criteoBidsIidArray))
      .map(keyAdscaleResponse(_))
//    keyedAdscaleBidsRDD.top(10).foreach(println)
    println(s"${keyedAdscaleBidsRDD.count()} adscale bid responses")

    // Get openRTB bids > 0 for these criteo bids > 0
    val keyedOpenRtbBidsRDD = sparkContext.textFile(openRtbResponseFile, 2)
      .filter(filterContainsString(_, criteoBidsIidArray))
      .filter(!_.contains("\"http-status\":204"))
      .filter(!_.contains("\"tpid\":58")) // and not 58 as it has pretty printing on.
      .map(keyOpenRtbResponse(_))
//    keyedOpenRtbBidsRDD.top(10).foreach(println)
    println(s"${keyedOpenRtbBidsRDD.count()} openRtb bids > 0")

    // Get appnexus responses > 0 for criteo bids > 0
    val keyedAppnexusBidsRDD = sparkContext.textFile(appnexusResponseFile, 2)
      .filter(filterContainsString(_, criteoBidsIidArray))
      .filter(_.contains("\"no_bid\":false"))
      .map(keyAppnexusBids(_))
//    keyedAppnexusBidsRDD.top(10).foreach(println)
    println(s"${keyedAppnexusBidsRDD.count()} appnexus bids > 0")
    
    // Now get all bid requests together
    val allKeyedBidsRDD = keyedAdscaleBidsRDD.union(keyedOpenRtbBidsRDD).union(keyedAppnexusBidsRDD).groupByKey()
    allKeyedBidsRDD.top(10).foreach(println)
    println(s"${allKeyedBidsRDD.count()} all bids")
    
    // Now sort the bids by price.
    var countCriteoWins = 0
    for (e <- allKeyedBidsRDD) {
      val sortedBids = e._2.toList.sortBy(_._2).reverse
      if (sortedBids.head._1.equals(criteoId)) 
        countCriteoWins += 1
//      println(s"${e._1} - ${sortedBids}")
        println(countCriteoWins)
    }
    println(countCriteoWins)
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
    val price = json.response.seatbid(0).bid(0).price
    (json.response.id.substring(0, 20), (json.tpid.toString(), price))
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
}