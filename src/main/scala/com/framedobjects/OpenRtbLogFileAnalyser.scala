package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.jackson.Json
import com.framedobjects.model.OpenRtbResponseLogEntry
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.framedobjects.model.OpenRtbRequestLogEntry
import java.io.PrintWriter
import java.io.File
import scala.util.Sorting

object OpenRtbLogFileAnalyser {

  def main(args: Array[String]) {
    val startDateTime = new DateTime(2014, 12, 17, 8, 49, 0, DateTimeZone.UTC)
    val endDateTime = new DateTime(2014, 12, 17, 8, 59, 0, DateTimeZone.UTC)

    val responseFileName = "/users/jensr/Documents/DevNotes/investigations/openrtb/17122014/new-openrtb-bidresponse-*.log.gz"
    val requestFileName = "/users/jensr/Documents/DevNotes/investigations/openrtb/17122014/new-openrtb-bidrequest-*.log.gz"
    val responseResultFile = "/users/jensr/Documents/DevNotes/investigations/openrtb/17122014/bid-responses.txt"
    val requestResultFile = "/users/jensr/Documents/DevNotes/investigations/openrtb/17122014/bid-request.txt"

    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("OpenRTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val requestRawRDD = sparkContext.textFile(requestFileName, 2)
    println(s"request raw count: ${requestRawRDD.count}")
    val requestJsonRDD = requestRawRDD.map(OpenRtbRequestLogEntry.fromJson(_)).filter(x => filterByTime(x.time, startDateTime, endDateTime))
    println(s"request json count: ${requestJsonRDD.count}")
    
    val requestTextRDD = requestJsonRDD.map(convertRequestToText(_))
    val requestTextArray = requestTextRDD.toArray
    Sorting.quickSort(requestTextArray)
    
    writeResultFile(requestResultFile, requestTextArray)

    val responseRawRDD = sparkContext.textFile(responseFileName, 2)
    println(s"response raw count: ${responseRawRDD.count}")

    val responseJsonRDD = responseRawRDD.filter(filterNewLineInResponse(_)).map(convertResponseToJson(_)).filter(x => filterByTime(x.time, startDateTime, endDateTime))
    println(s"response json count: ${responseJsonRDD.count}")

    val responseTextRDD = responseJsonRDD.map(convertResponseToText(_))
    val responseTextArray = responseTextRDD.toArray
    Sorting.quickSort(responseTextArray)

    writeResultFile(responseResultFile, responseTextArray)

    sparkContext.stop
  }

  def isBefore(checkTime: Long, againstTime: DateTime): Boolean = {
    getUtcDateTime(checkTime).compareTo(againstTime) < 0
  }

  def isAfterIncluding(checkTime: Long, againstTime: DateTime): Boolean = {
    getUtcDateTime(checkTime).compareTo(againstTime) >= 0
  }

  private def filterNewLineInResponse(logEntry: String): Boolean = {
    !logEntry.contains("\\n")
  }

  private def writeResultFile(responseResultFile: String, array: Array[String]) {
    val writer = new PrintWriter(new File(responseResultFile))
    array.foreach(entry => writer.write(s"${entry}\n"))
    writer.flush()
    writer.close()
  }

  private def convertResponseToText(entry: OpenRtbResponseLogEntry): String = {
    s"${getUtcDateTime(entry.time)}, ${entry.time}, ${entry.response.id}, ${entry.response.seatbid(0).bid(0).id}, ${entry.response.seatbid(0).bid(0).price}"
  }
  
  private def convertRequestToText(entry: OpenRtbRequestLogEntry): String = {
    s"${getUtcDateTime(entry.time)}, ${entry.time}, ${entry.request.id}, ${entry.request.user.id}, ${entry.request.site.publisher.id}, ${entry.request.site.id}"
  }

  private def convertResponseToJson(logEntry: String): OpenRtbResponseLogEntry = {
    OpenRtbResponseLogEntry.fromJson(logEntry)
  }

  private def filterByTime(logTime: Long, from: DateTime, to: DateTime): Boolean = {
    isAfterIncluding(logTime, from) && isBefore(logTime, to)
  }

  private def getUtcDateTime(time: Long): DateTime = {
    new DateTime(time, DateTimeZone.UTC)
  }
}