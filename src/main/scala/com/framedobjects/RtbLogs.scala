package com.framedobjects

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object RtbLogs {

  type ResponseByIid = (Long, RtbResponse)
  type NotificationByIid = (Long, String)
  type CountByIid = (Long, String)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS")

  val startDate = dateFormat.parse("2014-10-29 20:00:00,000")
  val endDate = dateFormat.parse("2014-10-29 21:10:00,000")

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val bidResponsesByIidRDD: RDD[CountByIid] = getRtbResponseRDDKeyedByImpressionId(sparkContext)
    val notificationsByIidRDD = getRtbNotificationsRDDKeyedByImpressionId(sparkContext)
    val intersectionRDD = bidResponsesByIidRDD.intersection(notificationsByIidRDD);
//    val unfilteredResult = bidResponsesByIidRDD.leftOuterJoin(intersectionRDD)
//    val filteredResult = unfilteredResult.filter(filterForNone)
//    
//    filteredResult.foreach(println)
    
//    println(s"bid responses: ${bidResponsesByIidRDD.count}\nintersection: ${intersectionRDD.count}\nunfilteredResult: ${unfilteredResult.count}\nfilteredResult: ${filteredResult.count}")
    
    val deltaRDD = bidResponsesByIidRDD.subtractByKey(intersectionRDD)
    deltaRDD.foreach(println)
    deltaRDD.persist(StorageLevel.MEMORY_AND_DISK)
    
    println(s"bid responses: ${bidResponsesByIidRDD.count}\nintersection: ${intersectionRDD.count}\ndeltaRDD: ${deltaRDD.count}")
    
    sparkContext.stop
  }
  
  def filterForNone(all: (Long, (String, Option[String]))): Boolean = {
    all._2._2 match {
      case None => true
      case _ : Option[String] => false
    }
  }

  def getRtbResponseRDDKeyedByImpressionId(sparkContext: SparkContext): RDD[CountByIid] = {
    val responsesFileRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_responses-436.log", 2)
    val filteredBidResponseRDD = responsesFileRDD.filter(filterForAdverts(_, startDate, endDate))
    filteredBidResponseRDD.map(mapResponseJsonToIIdKey(_))
  }

  def getRtbNotificationsRDDKeyedByImpressionId(sparkContext: SparkContext):RDD[CountByIid] = {
    val notificationFileRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_notif-436.log", 2)
    val filteredNotificationRDD = notificationFileRDD.filter(notificationFilter(_, startDate, endDate))
    filteredNotificationRDD.map(mapNotificationJsonToIIdKey)
  }

  def filterForAdverts(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))
    if (logTime.after(startDate) && logTime.before(endDate)) {
      line match {
        case s if (s.contains("\"aid\":137519,") || s.contains("\"aid\":137520,") || s.contains("\"aid\":137521,")) => true
        case _ => false
      }
    } else false
  }

  def notificationFilter(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))

    logTime.after(startDate)// && logTime.before(endDate)
  }

  def createKeys(logData: RDD[String]): RDD[CountByIid] = {
    logData.map(mapResponseJsonToIIdKey(_))
  }

  def mapResponseJsonToIIdKey(jsonLine: String): CountByIid = {
    val rtbResponse = RtbResponse.fromJson(jsonLine.split("\\|")(1))
    (rtbResponse.impression_id, "0")
  }

  def mapNotificationJsonToIIdKey(jsonLine: String): CountByIid = {
    val iidString = jsonLine.split("\\|")(1)
    (iidString.toLong, "0")
  }
}