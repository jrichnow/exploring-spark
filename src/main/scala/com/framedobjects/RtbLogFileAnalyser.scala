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

object RtbLogFileAnalyser {

  type CountByIid = (Long, String)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS")

  //  val startDate = dateFormat.parse("2014-10-29 20:00:00,000")
  //  val endDate = dateFormat.parse("2014-10-29 21:10:00,000")
  //  val responseFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_responses-*.log"
  //  val notificationFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/opt_notif-*.log"

  val startDate = dateFormat.parse("2014-11-12 23:00:00,000")
  val endDate = dateFormat.parse("2014-11-12 23:59:59,000")
  val responseFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/12112014/opt_responses-433--2014-11-12--*.log"
  val notificationFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/12112014/opt_notif-433--2014-11-12--*.log"

  val resultFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/12112014/result_23-24.txt"

  val campaignAdvertMap = Map("47247" -> List("137519", "137520", "137521"),
    "38395" -> List("111875", "111876"),
    "34495" -> List("99510", "99518", "99519", "99520", "99521"),
    "34496" -> List("99511", "99522", "99523", "99524", "99525"),
    "34497" -> List("99512", "99527", "99529", "99526", "99528"),
    "35433" -> List("102794", "102796", "102797", "102798", "102799", "112085"),
    "38575" -> List("112444", "112445"),
    "46172" -> List("134836", "134835", "134834"),
    "46271" -> List("135050", "135049", "135048"),
    "46591" -> List("135881", "135880", "135879"),
    "44587" -> List("137796", "132149", "131316", "131317", "129859", "130212"),
    "44863" -> List("137794", "137140", "130885", "131806", "130882"),
    "22108" -> List("60456", "62510", "81217", "60457", "60458", "81216", "81215", "99545", "99547"))

  val jsonfiedCampaignAdvertMap = campaignAdvertMap.mapValues(x => x.map(a => "\"aid\":" + a + ",\""))

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    println("fetching bid responses ...")
    val responsesFileRDD = sparkContext.textFile(responseFileName, 2)
    responsesFileRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("fetching notifications ...")
    val notificationsByIidRDD = getRtbNotificationsRDDKeyedByImpressionId(sparkContext)
    notificationsByIidRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val writer = new PrintWriter(new File(resultFile))
    writer.write("cid\tresp\tnotif\tdelta\tperc\tiids\n")

    for (key <- jsonfiedCampaignAdvertMap.keySet) {
      val (responses, notifications, delta, iids) = process(key, responsesFileRDD, notificationsByIidRDD)
      val perc = calculatePercentage(responses, notifications)
      val percEval: Double = perc match {
        case a if a.isNaN() => 0
        case x => x
      }
      println(s"campaign: $key - perc: $perc, percEval: $percEval")
      val percentage = BigDecimal(percEval).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      writer.write(s"$key\t$responses\t$notifications\t$delta\t$percentage\t$iids\n")
    }

    writer.close

    sparkContext.stop
  }

  def process(campaignId: String, responsesRDD: RDD[String], notificationRDD: RDD[(Long, String)]): (Long, Long, Long, String) = {
    val bidResponsesByIidRDD: RDD[CountByIid] = getRtbResponseRDDKeyedByImpressionId(responsesRDD, campaignAdvertMap.get(campaignId).get)
    val intersectionRDD = bidResponsesByIidRDD.intersection(notificationRDD);
    val deltaRDD = bidResponsesByIidRDD.subtractByKey(intersectionRDD)

    (bidResponsesByIidRDD.count, intersectionRDD.count, deltaRDD.count, deltaRDD.keys.toArray.mkString(","))
  }

  def calculatePercentage(a: Long, b: Long): Double = {
    (100 * (a - b)) / a.toDouble
  }

  def getRtbResponseRDDKeyedByImpressionId(lines: RDD[String], advertList: List[String]): RDD[CountByIid] = {
    val filteredBidResponseRDD = lines.filter(filterByAdvertsAndTime(_, startDate, endDate, advertList))
    filteredBidResponseRDD.map(mapResponseJsonToIIdKey(_))
  }

  def getRtbNotificationsRDDKeyedByImpressionId(sparkContext: SparkContext): RDD[CountByIid] = {
    val notificationFileRDD = sparkContext.textFile(notificationFileName, 2)
    val filteredNotificationRDD = notificationFileRDD.filter(notificationFilterByTime(_, startDate, endDate))
    filteredNotificationRDD.map(mapNotificationJsonToIIdKey)
  }

  def filterByAdvertsAndTime(line: String, startDate: Date, endDate: Date, advertList: List[String]): Boolean = {
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

  def notificationFilterByTime(line: String, startDate: Date, endDate: Date): Boolean = {
    val lineSplit = line.split("\\|")
    val logTime = dateFormat.parse(lineSplit(0))

    logTime.after(startDate) // && logTime.before(endDate)
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