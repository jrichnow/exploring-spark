package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.framedobjects.model.AppnexusRequestLogEntry
import java.io.PrintWriter
import java.io.File

object AppnexusPlacementLoggerAnalyser {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/appnexus/logs"
  val requestHttpFileName = s"$investigationRootFolder/dsp-appnexus-bidrequest-401.log"
  val responseHttpFileName = s"$investigationRootFolder/dsp-appnexus-bidresponse-401.log"
  val resultFileName = s"$investigationRootFolder/appnexus_error_5_requests.log"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val filteredResponseRDD = sparkContext.textFile(responseHttpFileName).filter(_.contains("request_error_id\":5"));
    val keyedResponseRDD = filteredResponseRDD.map(keyResponse(_))
    keyedResponseRDD.top(10).foreach(println)
    println(keyedResponseRDD.count)

    val keyedRequestsRDD = sparkContext.textFile(requestHttpFileName).map(keyRequest(_))
    keyedRequestsRDD.top(5).foreach(println)
    println(keyedRequestsRDD.count)

    val joinedRDD = keyedResponseRDD.join(keyedRequestsRDD)
    joinedRDD.top(5).foreach(println)
    println(joinedRDD.count)

    val jsonRequestRDD = joinedRDD.mapValues(mapJoinedValues(_)).values
    jsonRequestRDD.top(5).foreach(println)
    println(jsonRequestRDD.count)
    
    writeResultFile(resultFileName, jsonRequestRDD.toArray)
  }

  private def keyResponse(line: String): (String, String) = {
    val array = line.split(", ")
    (array(0), array(2))
  }

  private def keyRequest(line: String): (String, String) = {
    val array = line.split(", ")
    (array(0), array(1))
  }

  private def mapJoinedValues(map: (String, String)): String = {
    val array = map._2.split("},")
    if (array(0).contains("request_error_id\":5"))
      array(1)
    else
      array(0)
  }
  
  private def writeResultFile(responseResultFile: String, array: Array[String]) {
    val writer = new PrintWriter(new File(responseResultFile))
    array.foreach(entry => writer.write(s"${entry}\n"))
    writer.flush()
    writer.close()
  }
}