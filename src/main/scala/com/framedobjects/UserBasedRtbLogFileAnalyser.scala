package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.OpenRtbResponseLogEntry
import com.framedobjects.model.OpenRtbRequestLogEntry

object UserBasedRtbLogFileAnalyser {

  val userId = "522211416365455281"
  val impressionId = "422951429239130048"

  val rootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-538b/logs"
  val openBidRequestFiles = s"$rootFolder/new-openrtb-bidrequest-422.log"
  val httpBidRequestFiles = s"$rootFolder/http-bidrequest-422*"
  val httpBidResponseFiles = s"$rootFolder/http-bidresponse-422*"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("OpenRTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val openBidRequestRawRDD = sparkContext.textFile(openBidRequestFiles, 2)
    val filteredByUserOpenBidRequestRawRDD = openBidRequestRawRDD.filter(_.contains(userId)).filter(_.contains(impressionId))
    filteredByUserOpenBidRequestRawRDD.foreach(println)

    // Get the distinct impression Ids.
    val impressionIds = filteredByUserOpenBidRequestRawRDD.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id).map(_.substring(0, 18)).distinct
    for (impressionId <- impressionIds.toArray) {
      getAllHttpBidRequests(sparkContext, impressionId)
      getAllHttpBidResponses(sparkContext, impressionId)
    }
    //    val filteredByUserAndPmpOpenBidRequestsRDD = filteredByUserOpenBidRequestRawRDD.filter(_.contains("\"pmp\":"))
    //    filteredByUserAndPmpOpenBidRequestsRDD.foreach(println)

  }

  private def getAllHttpBidRequests(sparkContext: SparkContext, impressionId: String) {
    val httpBidRequestRawRDD = sparkContext.textFile(httpBidRequestFiles, 2)
    val filteredHttpBidRequestRawRDD = httpBidRequestRawRDD.filter(_.contains(userId)).filter(_.contains(impressionId))
    filteredHttpBidRequestRawRDD.foreach(println)
  }

  private def getAllHttpBidResponses(sparkContext: SparkContext, impressionId: String) {
    val httpBidRequestRawRDD = sparkContext.textFile(httpBidResponseFiles, 2)
    val filteredHttpBidRequestRawRDD = httpBidRequestRawRDD.filter(_.contains(impressionId))
    filteredHttpBidRequestRawRDD.foreach(println)
  }
}