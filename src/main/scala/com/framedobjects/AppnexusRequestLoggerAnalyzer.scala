package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AppnexusRequestLoggerAnalyzer {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/appnexus/logs"
  val responseHttpFileName = s"$investigationRootFolder/dsp-appnexus-bidrequest-401*"

  val blacklist = Seq(
    "ad-button.de",
    "alo.rs",
    "chefmail.ch",
    "chrome-extension",
    "ebesucher.de",
    "file://",
    "flirtarea.de",
    "foxmails24.de",
    "google.com",
    "http://144.76.78.162",
    "http://46.4.16.4",
    "meinvz.net",
    "Miet24.de",
    "myadvertisingpays.com",
    "privatelink.de",
    "safari-reader://",
    "turbotrafficbooster.com",
    "v2load.de")

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val rawResponseRDD = sparkContext.textFile(responseHttpFileName);
    println(rawResponseRDD.count)

    val filteredBlacklistRDD = rawResponseRDD.filter(filterBlacklist(_))
    println(s"blacklisted count: ${filteredBlacklistRDD.count}")
    filteredBlacklistRDD.foreach(println)
  }

  private def filterBlacklist(line: String): Boolean = {
    for (entry <- blacklist) yield {
      if (line.contains(entry)) return true
    }
    false
  }
}