package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.jackson.Json
import com.framedobjects.model.OpenRtbResponseLogEntry
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.framedobjects.model.OpenRtbRequestLogEntry

object OpenRtbLogFileAnalyser {

  def main(args: Array[String]) {
    val startDateTime = new DateTime(2014, 12, 9, 9, 0, 0, DateTimeZone.UTC)
    val endDateTime = new DateTime(2014, 12, 9, 9, 17, 27, DateTimeZone.UTC)

    val responseFileName = "/users/jensr/Documents/DevNotes/investigations/openrtb/open-rtb-responses-*.log.gz"
    val requestFileName = "/users/jensr/Documents/DevNotes/investigations/openrtb/open-rtb-requests-*.log.gz"

    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("OpenRTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val requestRawRDD = sparkContext.textFile(requestFileName, 2)
    println(s"request raw count: ${requestRawRDD.count}")
    val requestJsonRDD = requestRawRDD.map(OpenRtbRequestLogEntry.fromJson(_)).filter(filterRequestsByTime(_, startDateTime, endDateTime))
    println(s"request json count: ${requestJsonRDD.count}")

    val responseRawRDD = sparkContext.textFile(responseFileName, 2)
    println(s"response raw count: ${responseRawRDD.count}")
    val responseJsonRDD = responseRawRDD.map(convertResponseToJson(_)).filter(filterResponsesByTime(_, startDateTime, endDateTime))
    println(s"response json count: ${responseJsonRDD.count}")

    sparkContext.stop
  }

  def isBefore(checkTime: Long, againstTime: DateTime): Boolean = {
    getUtcDateTime(checkTime).compareTo(againstTime) < 0
  }

  def isAfterIncluding(checkTime: Long, againstTime: DateTime): Boolean = {
    getUtcDateTime(checkTime).compareTo(againstTime) >= 0
  }

  private def convertResponseToJson(logEntry: String): OpenRtbResponseLogEntry = {
    OpenRtbResponseLogEntry.fromJson(logEntry)
  }

  private def filterResponsesByTime(entry: OpenRtbResponseLogEntry, from: DateTime, to: DateTime): Boolean = {
    isAfterIncluding(entry.time, from) && isBefore(entry.time, to)
  }

  private def filterRequestsByTime(entry: OpenRtbRequestLogEntry, from: DateTime, to: DateTime): Boolean = {
    isAfterIncluding(entry.time, from) && isBefore(entry.time, to)
  }

  private def getUtcDateTime(time: Long): DateTime = {
    new DateTime(time, DateTimeZone.UTC)
  }
}