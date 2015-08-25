package com.framedobjects.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.framedobjects.model.IhAccessLog
import java.net.URLDecoder
import java.io.PrintWriter
import java.io.File

object SimpleIhAccessLogger {

  val slotIds = Seq(108258, 108243, 108322, 106057, 90220, 106072, 108232, 91449, 108212, 106220)
  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1182/"
  val logFileName = s"$investigationRootFolder/logs/20150824/ih-access-401--2015-08-24--0.log.gz"

  def main(args: Array[String]) {

    val sparkConfig = new SparkConf().setMaster("local").setAppName("IhAccessLog Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val jsonRDD = sparkContext.textFile(logFileName).filter(_.contains("\"sid\":73408")).map(IhAccessLog.fromJson(_))
    val pupImprJsonRDD = jsonRDD.filter(_.query.contains("pup=true")).filter(filterImpr(_))
    println(s"pup requests: ${pupImprJsonRDD.count()}")

    val pageReferrerRDD = pupImprJsonRDD.map(mapWsValue(_)).distinct().sortBy(x => x, true)
    pageReferrerRDD.foreach(println)
    
    writeResultFile(s"$investigationRootFolder/73408.txt", pageReferrerRDD.toArray())
  }

  private def filterSlotIds(line: String) {
    for (slotId <- slotIds) {
      line.contains("\"sid\": ")
    }
  }

  private def filterImpr(log: IhAccessLog): Boolean = {
    log.path match {
      case Some(p) if p.equals("/impr") => true
      case _ => false
    }
  }

  private def mapWsValue(log: IhAccessLog): String = {
    val queryParams = log.query.split("&")
    val wsParam = queryParams.filter(_.startsWith("ws"))
    wsParam match {
      case p if p.length ==1 => {
        val ws = p.head.split("=")
        ws match {
          case x if x.length == 2 => URLDecoder.decode(x(1))
          case _ => "no ws value"
        }
      }
      case _ => "no ws param"
    }
  }
  
  private def writeResultFile(responseResultFile: String, array: Array[String]) {
    val writer = new PrintWriter(new File(responseResultFile))
    array.foreach(entry => writer.write(s"${entry}\n"))
    writer.flush()
    writer.close()
  }
}