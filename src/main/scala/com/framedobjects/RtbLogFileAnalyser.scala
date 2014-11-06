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

object RtbLogFileAnalyser {

  type ResponseByIid = (Long, RtbResponse)
  type NotificationByIid = (Long, String)
  type CountByIid = (Long, String)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS")

  val startDate = dateFormat.parse("2014-10-29 20:00:00,000")
  val endDate = dateFormat.parse("2014-10-29 21:10:00,000")

  val campaignMap = Map("47247" -> List("137519", "137520", "137521"),
    "38395" -> List("111875", "111876"),
    "34495" -> List("99510", "99518", "99519", "99520", "99521"),
    "34497" -> List("99512", "99527", "99529", "99526", "99528"),
    "38575" -> List("112444", "112445"),
    "22108" -> List("60456", "62510", "81217", "60457", "60458", "81216", "81215", "99545", "99547"))
    

  val jsonfiedCampaignMap = campaignMap.mapValues(x => x.map(a => "\"aid\":" + a + ",\""))
  jsonfiedCampaignMap.foreach(println)

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    println("fetching bid responses ...")
    val responsesFileRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_responses-436.log", 2)
    responsesFileRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("fetching notifications ...")
    val notificationsByIidRDD = getRtbNotificationsRDDKeyedByImpressionId(sparkContext)
    notificationsByIidRDD.persist(StorageLevel.MEMORY_AND_DISK)

    for (key <- jsonfiedCampaignMap.keySet) {
      val bidResponsesByIidRDD: RDD[CountByIid] = getRtbResponseRDDKeyedByImpressionId(responsesFileRDD, campaignMap.get(key).get)
      val intersectionRDD = bidResponsesByIidRDD.intersection(notificationsByIidRDD);
      val deltaRDD = bidResponsesByIidRDD.subtractByKey(intersectionRDD)

      val r = bidResponsesByIidRDD.count
      val n = intersectionRDD.count
      val d = deltaRDD.count
      val p = BigDecimal(percentage(r, n)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      println(s"cid: $key,\tresponses: $r\tnotifications: $n\tdelta: $d\tperc: $p\t${deltaRDD.keys.toArray.mkString(",")}")
    }

    sparkContext.stop
  }
  
  def percentage(a: Long, b: Long): Double = {
    (100 * (a-b)) / a.toDouble
  }

  def getRtbResponseRDDKeyedByImpressionId(lines: RDD[String], advertList: List[String]): RDD[CountByIid] = {
    val filteredBidResponseRDD = lines.filter(filterForAdverts(_, startDate, endDate, advertList))
    filteredBidResponseRDD.map(mapResponseJsonToIIdKey(_))
  }

  def getRtbNotificationsRDDKeyedByImpressionId(sparkContext: SparkContext): RDD[CountByIid] = {
    val notificationFileRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_notif-436.log", 2)
    val filteredNotificationRDD = notificationFileRDD.filter(notificationFilter(_, startDate, endDate))
    filteredNotificationRDD.map(mapNotificationJsonToIIdKey)
  }

  def filterForAdverts(line: String, startDate: Date, endDate: Date, advertList: List[String]): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))
    if (logTime.after(startDate) && logTime.before(endDate)) {
      line match {
        case s if (containsAdvertId(s, advertList)) => true
        case _ => false
      }
    } else false
  }

  def containsAdvertId(line: String, advertList: List[String]): Boolean = {
    for (x <- advertList) {
      if (line.contains(x)) return true
    }
    false
  }

  def notificationFilter(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))

    logTime.after(startDate) // && logTime.before(endDate)
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