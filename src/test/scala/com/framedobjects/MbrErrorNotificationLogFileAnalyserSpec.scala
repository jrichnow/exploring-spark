package com.framedobjects

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class MbrErrorNotificationLogFileAnalyserSpec extends FlatSpec with Matchers {

  "Analyser" should "correctly count data in log file" in {
    val startDate = RtbLogFileAnalyser.dateFormat.parse("2015-01-08 02:00:00,000")
    val endDate = RtbLogFileAnalyser.dateFormat.parse("2015-01-08 03:00:00,000")
    val fileName = "/users/jensr/development/exploring-spark/src/main/resources/opt_notif-error.log"
      
    MbrErrorNotificationLogFileAnalyser.processFile(fileName, startDate, endDate)
  }
}