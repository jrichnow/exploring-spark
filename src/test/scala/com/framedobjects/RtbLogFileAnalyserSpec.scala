package com.framedobjects

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.io.File
import java.io.FileReader
import scala.io.Source

class RtbLogFileAnalyserSpec extends FlatSpec with Matchers {

  "RTB log file analyser" should "correctly match responses with notifications" in {
    val startDate = RtbLogFileAnalyser.dateFormat.parse("2014-11-25 21:00:00,000")
    val endDate = RtbLogFileAnalyser.dateFormat.parse("2014-11-25 22:00:00,000")

    val responseFileName = "/users/jensr/development/exploring-spark/src/main/resources/opt_responses.log"
    val notificationFileName = "/users/jensr/development/exploring-spark/src/main/resources/opt_notif.log"

    val resultFileName = "/users/jensr//development/exploring-spark/src/main/resources/result.txt"

    val campaignAdvertMap = Map("38575" -> List("112444", "112445"),
        "38395" -> List("111875", "111876"),
        "44444" -> List("123456"))

    RtbLogFileAnalyser.process(responseFileName, notificationFileName, resultFileName, startDate, endDate, campaignAdvertMap)

    Source.fromFile(resultFileName).getLines.foreach(line => line match {
      case a if (a.startsWith("cid")) => println("ignore")
      case b if (b.startsWith("38575")) => assertResult(b, 2, 2, 0, 0.0, "")
      case c if (c.startsWith("38395")) => assertResult(c, 2, 1, 1, 50.0, "433721416950726537")
      case d if (d.startsWith("44444")) => assertResult(d, 3, 0, 3, 100.0, "433721416950726538,433721416950726539,433721416950726540")
    })
  }

  def assertResult(resultLine: String, responses: Int, notifications: Int, delta: Int, percentage: Double, iids: String) {
    val resultArray = resultLine.split("\t")

    resultArray(1).toInt should be(responses)
    resultArray(2).toInt should be(notifications)
    resultArray(3).toInt should be(delta)
    resultArray(4).toDouble should be(percentage)

    if (iids.isEmpty()) {
      resultArray.length should be(5)
    } else {
      val actualIids = resultArray(5).split(",")
      val expectedIids = iids.split(",")
      
      actualIids.length should be (expectedIids.length)
      
      expectedIids.foreach(iid => actualIids should contain (iid))
    }
  }
}