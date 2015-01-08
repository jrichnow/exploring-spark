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

  def main(args: Array[String]) {
    val startDate = dateFormat.parse("2014-12-02 20:00:00,000")
    val endDate = dateFormat.parse("2014-12-02 21:00:00,000")

    val responseFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/02122014/opt_responses-435--2014-12-02--*.log.gz"
    val notificationFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/02122014/opt_notif-435--2014-12-02--*.log.gz"

    val resultFileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/02122014/result_20-21.txt"

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

    process(responseFileName, notificationFileName, resultFileName, startDate, endDate, campaignAdvertMap)
  }

  def process(responseFileName: String, notificationFileName: String, resultFileName: String, startDate: Date, endDate: Date, campaignAdvertMap: Map[String, List[String]]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    println("fetching bid responses ...")
    val responsesFileRDD = sparkContext.textFile(responseFileName, 2)
    responsesFileRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("fetching notifications ...")
    val jsonfiedCampaignAdvertMap = campaignAdvertMap.mapValues(x => x.map(a => "\"aid\":" + a + ",\""))

    val notificationsByIidRDD = getRtbNotificationsRDDKeyedByImpressionId(sparkContext, notificationFileName, startDate, endDate)
    notificationsByIidRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val writer = new PrintWriter(new File(resultFileName))
    writer.write("cid\tresp\tnotif\tdelta\tperc\tiids\n")

    for (campaignId <- jsonfiedCampaignAdvertMap.keySet) {
      val (responses, notifications, delta, iids) = processCampaign(campaignId, startDate, endDate, jsonfiedCampaignAdvertMap.get(campaignId).get, responsesFileRDD, notificationsByIidRDD)
      val perc = calculatePercentage(responses, notifications)
      val percEval: Double = perc match {
        case a if a.isNaN() => 0
        case x => x
      }
      println(s"campaign: $campaignId - perc: $perc, percEval: $percEval")
      val percentage = BigDecimal(percEval).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      writer.write(s"$campaignId\t$responses\t$notifications\t$delta\t$percentage\t$iids\n")
    }

    writer.close

    sparkContext.stop
  }

  def processCampaign(campaignId: String, startDate: Date, endDate: Date, jsonfiedAdvertList: List[String], responsesRDD: RDD[String], notificationRDD: RDD[(Long, String)]): (Long, Long, Long, String) = {
    val bidResponsesByIidRDD: RDD[CountByIid] = getRtbResponseRDDKeyedByImpressionId(startDate, endDate, responsesRDD, jsonfiedAdvertList)
    // Temporary saving iids for certain campaign id
    campaignId match {
      case "38575" => {
        val fileName = "/users/jensr/Documents/DevNotes/investigations/sc-2666/02122014/38575-iids-20-21.txt"
        
        val writer = new PrintWriter(new File(fileName))
        notificationRDD.keys.toArray.foreach(entry => writer.println(entry))
        writer.flush()
        writer.close()
      }
      case _ => 
    }

    val intersectionRDD = bidResponsesByIidRDD.intersection(notificationRDD);
    val deltaRDD = bidResponsesByIidRDD.subtractByKey(intersectionRDD)

    (bidResponsesByIidRDD.count, intersectionRDD.count, deltaRDD.count, deltaRDD.keys.toArray.mkString(","))
  }

  private def calculatePercentage(a: Long, b: Long): Double = {
    (100 * (a - b)) / a.toDouble
  }

  private def getRtbResponseRDDKeyedByImpressionId(startDate: Date, endDate: Date, lines: RDD[String], jsonfiedAdvertList: List[String]): RDD[CountByIid] = {
    val filteredBidResponseRDD = lines.filter(filterByAdvertsAndTime(_, startDate, endDate, jsonfiedAdvertList))
    filteredBidResponseRDD.map(mapResponseJsonToIIdKey(_))
  }

  private def getRtbNotificationsRDDKeyedByImpressionId(sparkContext: SparkContext, notificationFileName: String, startDate: Date, endDate: Date): RDD[CountByIid] = {
    val notificationFileRDD = sparkContext.textFile(notificationFileName, 2)
    val filteredNotificationRDD = notificationFileRDD.filter(notificationFilterByTime(_, startDate, endDate))
    filteredNotificationRDD.map(mapNotificationJsonToIIdKey)
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

    logTime.after(startDate) // && logTime.before(endDate)
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