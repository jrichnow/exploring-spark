package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date
import java.text.SimpleDateFormat
import com.framedobjects.model.Notification

object MbrErrorNotificationLogFileAnalyser {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS")

  def main(args: Array[String]) {
    val startDate = RtbLogFileAnalyser.dateFormat.parse("2015-01-09 20:00:00,000")
    val endDate = RtbLogFileAnalyser.dateFormat.parse("2015-01-09 22:00:00,000")

    val fileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/opt_notif-error-435*.log.gz"

    processFile(fileName, startDate, endDate)
  }

  def processFile(fileName: String, startDate: Date, endDate: Date) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("MBR Notification Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val errorNotifFileRDD = sparkContext.textFile(fileName, 2).filter(_.contains("BidOptimisationWinLossNotification")).filter(filterLogFileByTime(_, startDate, endDate))
    val notificationRDD = errorNotifFileRDD.map(mapToNotification)

    val advertRDD = notificationRDD.filter(_.winningAdvertId == 140246)
    val winningAdvertRDD = advertRDD.filter(_.win)

    println(s"# error notifications: ${errorNotifFileRDD.count}")
    println(s"# advert notifications: ${advertRDD.count}")
    println(s"# winning advert notifications: ${winningAdvertRDD.count}")
  }

  private def filterLogFileByTime(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))

    logTime.after(startDate) && logTime.before(endDate)
  }

  private def mapToNotification(logLine: String): Notification = {
    Notification.fromLogEntry(logLine)
  }
}