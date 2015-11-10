package com.framedobjects

import java.io.{File, PrintWriter}

import com.framedobjects.model.{OpenRtbResponseLogEntrySimple, OpenRtbRequestLogEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object VideoOpenRTB {

  def main(args: Array[String]) {
    val investigationFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1457"
    val requestFile = s"$investigationFolder/logs/video-request-38-304.json"
    val responseFile = s"$investigationFolder/logs/dsp-openrtb-bidresponse-304.log"
    val filteredResponsesOutFile = s"$investigationFolder/video-responses-38-304.log"

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Click Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val requestRDD = sparkContext.textFile(requestFile)
    println(s"requestRDD: ${requestRDD.count()}")

    val requestsKeyedByIdRDD = requestRDD.map(keyRequests)
    println(s"requestsKeyedByIdRDD: ${requestsKeyedByIdRDD.count()}")

    val responseRDD = sparkContext.textFile(responseFile)
    val responsesKeyedByIdRDD = responseRDD.map(keyResponses)

    val combinedKeyedByIdRDD = requestsKeyedByIdRDD.join(responsesKeyedByIdRDD)
    println(s"combinedKeyedByIdRDD: ${combinedKeyedByIdRDD.count()}")

    val filteredResponsesRDD = combinedKeyedByIdRDD.map(l => l._2._2)
    println(s"filteredResponsesRDD: ${filteredResponsesRDD.count()}")
    filteredResponsesRDD.foreach(println)

    writeFile(filteredResponsesRDD, filteredResponsesOutFile)
  }

  private def keyRequests(logEntry: String): (String, String) = {
    (OpenRtbRequestLogEntry.fromJson(logEntry).request.id, logEntry)
  }

  private def keyResponses(logEntry: String): (String, String) = {
    (OpenRtbResponseLogEntrySimple.fromJson(logEntry).requestId, logEntry)
  }

  private def writeFile(rdd: RDD[String], outFile: String): Unit = {
    val writer = new PrintWriter(new File(outFile))
    rdd.toArray().foreach(writer.println(_))

    writer.close()
  }
}
