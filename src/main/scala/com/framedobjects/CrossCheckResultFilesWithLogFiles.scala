package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CrossCheckResultFilesWithLogFiles {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Cross check ...")
    val sparkContext = new SparkContext(sparkConfig)

    val winningIidsSparkFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/48508/spark_48508_winning-iids-20-21.txt"
//        val winningIidsSparkFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/48508/spark_48508_delta-iids-20-21.txt"
    val responseFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/opt_responses-435-*.log.gz"
    val notificationFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/opt_notif-435-*.log.gz"

    val winningIidsRDD = sparkContext.textFile(winningIidsSparkFile, 2)
    println(winningIidsRDD.count)
    val winningIidsArray = winningIidsRDD.toArray

    //    processResponseFiles(sparkContext, responseFileName, winningIidsArray)
    processNotificationFiles(sparkContext, notificationFileName, winningIidsArray)

    sparkContext.stop
  }

  private def processResponseFiles(sparkContext: SparkContext, responseFileName: String, winningIidsArray: Array[String]) {
    val responsesRDD = sparkContext.textFile(responseFileName, 2);
    val filteredResponsesRDD = responsesRDD.filter(filterResponseImpressionId(_, winningIidsArray))
    println(filteredResponsesRDD.count)
    filteredResponsesRDD.foreach(println)
  }

  private def processNotificationFiles(sparkContext: SparkContext, notificationFileName: String, winningIidsArray: Array[String]) {
    val notificationsRDD = sparkContext.textFile(notificationFileName, 2);
    val filteredNotificationsRDD = notificationsRDD.filter(filterNotificationImpressionId(_, winningIidsArray))
    println(filteredNotificationsRDD.count)
    filteredNotificationsRDD.foreach(println)
  }

  private def filterResponseImpressionId(line: String, winningIidsArray: Array[String]): Boolean = {
    val rtbResponse = RtbResponse.fromJson(line.split("\\|")(1))
    winningIidsArray.contains(rtbResponse.impression_id.toString)
  }

  private def filterNotificationImpressionId(line: String, winningIidsArray: Array[String]): Boolean = {
    val impressionId = line.split("\\|")(1)
    winningIidsArray.contains(impressionId)
  }
}