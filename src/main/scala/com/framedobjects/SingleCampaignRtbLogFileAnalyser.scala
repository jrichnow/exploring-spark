package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import java.util.Date
import org.apache.spark.storage.StorageLevel
import java.lang.Double
import java.text.DecimalFormat
import java.io.FileWriter
import java.io.File
import java.io.PrintWriter

object SingleCampaignRtbLogFileAnalyser {

  type CountByIid = (Long, String)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS")
  val startHour = 00
  val endHour = 24
  val startDate = dateFormat.parse(s"2015-01-26 ${startHour}:00:00,000")
  val endDate = dateFormat.parse(s"2015-01-26 ${endHour}:00:00,000")

  val campaignId = 48693
  val advertIds = List(140651, 140652, 140653)
  val campaignAdvertMap = Map(campaignId -> advertIds)
  val jsonfiedCampaignAdvertMap = campaignAdvertMap.mapValues(x => x.map(a => "\"aid\":" + a + ",\""))

  val handlerInstance = 435;

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/sc-2666/26012015"

  val responseLogFileName = s"${investigationRootFolder}/logs/opt_responses-${handlerInstance}-*.log.gz"
  val notificationLogFileName = s"${investigationRootFolder}/logs/opt_notif-${handlerInstance}-*.log.gz"
    
  val resultFilePrefix = s"spark_${campaignId}_${handlerInstance}_${startHour}_${endHour}"
  val sparkResultFileName = s"${investigationRootFolder}/${campaignId}/${resultFilePrefix}-result.txt"

  def main(args: Array[String]) {
    createCampaignFolderIfRequired()
    
    val sparkConfig = new SparkConf().setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    println("fetching bid responses ...")
    val responsesFileRDD = sparkContext.textFile(responseLogFileName, 2)
    responsesFileRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("fetching notifications ...")

    val notificationsByIidRDD = getRtbNotificationsRDDKeyedByImpressionId(sparkContext, jsonfiedCampaignAdvertMap.get(campaignId).get)
    notificationsByIidRDD.persist(StorageLevel.MEMORY_AND_DISK)

//    val writer = new PrintWriter(new File(sparkResultFileName))
//    writer.write("cid\tresp\tnotif\tdelta\tperc\tiids not in notification\n")
//
//    for (campaignId <- jsonfiedCampaignAdvertMap.keySet) {
//      val (responses, notifications, delta, iids) = processCampaign(jsonfiedCampaignAdvertMap.get(campaignId).get, responsesFileRDD, notificationsByIidRDD)
//      val perc = calculatePercentage(responses, notifications)
//      val percEval: Double = perc match {
//        case a if a.isNaN() => 0
//        case x => x
//      }
//      println(s"campaign: $campaignId - perc: $perc, percEval: $percEval")
//      val percentage = BigDecimal(percEval).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
//      writer.write(s"$campaignId\t$responses\t$notifications\t$delta\t$percentage\t$iids\n")
//    }
//    writer.close

    sparkContext.stop
  }
  
  private def createCampaignFolderIfRequired() {
    val campaignFolder = new File(s"${investigationRootFolder}/${campaignId}")
    if (!campaignFolder.exists()) campaignFolder.mkdir()
  }

  private def processCampaign(jsonfiedAdvertList: List[String], responsesRDD: RDD[String], notificationRDD: RDD[(Long, String)]): (Long, Long, Long, String) = {
    val bidResponsesByIidRDD: RDD[CountByIid] = getRtbResponseRDDKeyedByImpressionId(responsesRDD, jsonfiedAdvertList)

    printResponseIidsForCampaign(bidResponsesByIidRDD)

    val intersectionRDD = bidResponsesByIidRDD.intersection(notificationRDD);
    val deltaRDD = bidResponsesByIidRDD.subtractByKey(intersectionRDD)

    (bidResponsesByIidRDD.count, intersectionRDD.count, deltaRDD.count, deltaRDD.keys.toArray.mkString(","))
  }

  private def printResponseIidsForCampaign(bidResponsesByIidRDD: RDD[CountByIid]) {
    val fileName = s"${investigationRootFolder}/${campaignId}/${resultFilePrefix}-response-iids.txt"

    val writer = new PrintWriter(new File(fileName))
    bidResponsesByIidRDD.keys.toArray.foreach(entry => writer.println(entry))
    writer.flush()
    writer.close()
  }
  
  private def printResponsesForCampaign(bidResponsesByIidRDD: RDD[String]) {
    val fileName = s"${investigationRootFolder}/${campaignId}/${resultFilePrefix}-responses.txt"

    val writer = new PrintWriter(new File(fileName))
    bidResponsesByIidRDD.toArray.foreach(entry => writer.println(entry))
    writer.flush()
    writer.close()
  }

  private def calculatePercentage(a: Long, b: Long): Double = {
    (100 * (a - b)) / a.toDouble
  }

  private def getRtbResponseRDDKeyedByImpressionId(lines: RDD[String], jsonfiedAdvertList: List[String]): RDD[CountByIid] = {
    val filteredBidResponseRDD = lines.filter(filterByAdvertsAndTime(_, startDate, endDate, jsonfiedAdvertList))
    printResponsesForCampaign(filteredBidResponseRDD)
    filteredBidResponseRDD.map(mapResponseJsonToIIdKey(_))
  }

  private def getRtbNotificationsRDDKeyedByImpressionId(sparkContext: SparkContext, jsonfiedAdvertList: List[String]): RDD[CountByIid] = {
    val notificationFileRDD = sparkContext.textFile(notificationLogFileName, 2)
    val filteredNotificationRDD = notificationFileRDD.filter(notificationFilterByTime(_, startDate, endDate)) //.filter(_.contains("win"))

    printWinningNotificationsForAdverts(filteredNotificationRDD, jsonfiedAdvertList)

    filteredNotificationRDD.map(mapNotificationJsonToIIdKey)
  }

  private def printWinningNotificationsForAdverts(winningNotificationsRDD: RDD[String], jsonfiedAdvertList: List[String]) {
    val winningNotificationsForAdvert = winningNotificationsRDD.filter(containsAdvertId(_, jsonfiedAdvertList))
    
    val fileNameWinNotifications = s"${investigationRootFolder}/${campaignId}/${resultFilePrefix}-win-notifications.txt"
    
    val winNotificationsWriter = new PrintWriter(new File(fileNameWinNotifications))
    winningNotificationsForAdvert.toArray.foreach(entry => winNotificationsWriter.println(entry))
    winNotificationsWriter.flush()
    winNotificationsWriter.close()
    
    val winningNotificationsIidsForAdvert = winningNotificationsForAdvert.map(mapNotificationJsonToIIdKey)

    val fileNameWinNotificationsIids = s"${investigationRootFolder}/${campaignId}/${resultFilePrefix}-win-notification-iids.txt"
    val winNotificationIidsWriter = new PrintWriter(new File(fileNameWinNotificationsIids))
    winningNotificationsIidsForAdvert.keys.toArray.foreach(entry => winNotificationIidsWriter.println(entry))
    winNotificationIidsWriter.flush()
    winNotificationIidsWriter.close()
  }

  private def filterByAdvertsAndTime(line: String, startDate: Date, endDate: Date, advertList: List[String]): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))
    if (logTime.after(startDate) && logTime.before(endDate)) {
      line match {
        case s if (containsAdvertId(s, advertList)) => true
        case _ => false
      }
    } else false
  }

  private def containsAdvertId(line: String, advertList: List[String]): Boolean = {
    for (x <- advertList) {
      if (line.contains(x)) return true
    }
    false
  }

  private def notificationFilterByTime(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))

    logTime.after(startDate) && logTime.before(endDate)
  }

  private def mapResponseJsonToIIdKey(jsonLine: String): CountByIid = {
    val rtbResponse = RtbResponse.fromJson(jsonLine.split("\\|")(1))
    (rtbResponse.impression_id, "0")
  }

  private def mapNotificationJsonToIIdKey(jsonLine: String): CountByIid = {
    val iidString = jsonLine.split("\\|")(1)
    (iidString.toLong, "0")
  }
}