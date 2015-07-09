package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.json4s.jackson.Json
import com.framedobjects.model.OpenRtbRequestLogEntry
import com.framedobjects.model.OpenRtbResponseLogEntry
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import org.apache.spark.storage.StorageLevel

object PmpOpenRtbAnalyser {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-981"
  val requestOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidrequest-434--2015-07-09--0.log.gz"
  val requestHttpFileName = s"$investigationRootFolder/logs/http-bidrequest-434--2015-07-09--1.log.gz"
  val responseOpenRtbFileName = s"$investigationRootFolder/logs/new-openrtb-bidresponse-434--2015-07-09--0.log.gz"
  val responseHttpFileName = s"$investigationRootFolder/logs/http-bidresponse-434--2015-07-09--0.log.gz"

  def main(args: Array[String]) {
    //  val dealIds = List("nynolvglon", "zxlkic4cnn", "77jgkhc4ca", "dgq2fznukl")
    val dealIds = List("zxlkic4cnn")

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    for (dealId <- dealIds) {
      val rawOpenRtbRequestRDD = sparkContext.textFile(requestOpenRtbFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawOpenRtbRequestByDealIdRDD = rawOpenRtbRequestRDD.filter(_.contains(dealId))
      val iids = rawOpenRtbRequestByDealIdRDD.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id).toArray.map(_.substring(0,20))
      
      val rawOpenRtbRequestByIidRDD = rawOpenRtbRequestRDD.filter(filterContainsString(_, iids)).map(mapByIidOpenRtbRequest(_))
      
      rawOpenRtbRequestByDealIdRDD.foreach(println)
      iids.foreach(println)
      rawOpenRtbRequestByIidRDD.foreach(println)
      
      // Get the entries from the HTTP requests.
      val rawHttpRequestRDD = sparkContext.textFile(requestHttpFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawHttpRequestByIidRDD = rawHttpRequestRDD.filter(filterContainsString(_, iids)).map(mapByIidHttpRequest(_))
      rawHttpRequestByIidRDD.foreach(println)
      
      // Get the entries from the OpenRtb responses.
      val rawOpenRtbResponseRDD = sparkContext.textFile(responseOpenRtbFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawOpenRtbResponseByIidRDD = rawOpenRtbResponseRDD.filter(filterContainsString(_, iids)).map(mapByIidOpenRtbResponse(_))
      rawOpenRtbResponseByIidRDD.foreach(println)
      
      // Get the entries from the Http responses.
      val rawHttpResponseRDD = sparkContext.textFile(responseHttpFileName, 2).persist(StorageLevel.MEMORY_AND_DISK)
      val rawHttpResponseByIidRDD = rawHttpResponseRDD.filter(filterContainsString(_, iids)).map(mapByIidHttpResponse(_))
      rawHttpResponseByIidRDD.foreach(println)
      
//      val combinedRequestRDD = rawOpenRtbRequestByIidRDD.union(rawHttpRequestByIidRDD).groupByKey
//      combinedRequestRDD.foreach(println)
      
      val combinedResponseRDD = rawOpenRtbResponseByIidRDD.union(rawHttpResponseByIidRDD).groupByKey
      combinedResponseRDD.foreach(println)
    }
  }

  private def writeResultFile(fileName: String, rdd: RDD[String]) {
    val writer = new PrintWriter(new File(fileName))
    rdd.toArray.foreach(line => writer.println(line))
    writer.flush()
    writer.close()
  }

  private def filterContainsString(line: String, list: Seq[String]): Boolean = {
    for (value <- list) {
      if (line.contains(value)) return true
    }
    false
  }
  
  private def mapByIidOpenRtbRequest(line: String): (String, String) = {
    (OpenRtbRequestLogEntry.fromJson(line).request.id.substring(0,20), line)
  }
  
  private def mapByIidHttpRequest(line: String): (String, String) = {
    (line.split(" ")(1).substring(0, 20), line)
  }
  
  private def mapByIidOpenRtbResponse(line: String): (String, String) = {
	  (OpenRtbResponseLogEntry.fromJson(line).response.id.substring(0,20), line)
  }
  
  private def mapByIidHttpResponse(line: String): (String, String) = {
	  (line.split(",")(0).substring(0, 20), line)
  }

  private def extractIids(a: (String, Iterable[String])): (String, Seq[String]) = {
    println(a._1, a._2.seq.size)
    a._2.foreach(println)
    val iidList = a._2.map(OpenRtbRequestLogEntry.fromJson(_)).map(_.request.id).map(_.substring(0, 20))
    iidList.foreach(println)
    (a._1, iidList.toSeq)
  }

  private def findAllRequestsByIid(rawRDD: RDD[String], iids: Iterable[String]) {
    println(iids.toList)
    println(rawRDD.count)
    val rawByIddRDD = rawRDD.filter(filterContainsString(_, iids.toList))
    //    rawByIddRDD.foreach(println)
  }
}