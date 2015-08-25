package com.framedobjects.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.framedobjects.model.IhAccessLog
import java.net.URLDecoder
import java.io.PrintWriter
import java.io.File

object NoWsValueSimpleIhAccessLogger {

  val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1182/"
  val logFileName = s"$investigationRootFolder/logs/20150824/ih-access-401--2015-08-24--*.log.gz"

  def main(args: Array[String]) {

    val sparkConfig = new SparkConf().setMaster("local").setAppName("IhAccessLog Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val jsonRDD = sparkContext.textFile(logFileName).filter(_.contains("\"sid\":116578")).map(IhAccessLog.fromJson(_))
    val pupImprJsonRDD = jsonRDD.filter(_.query.contains("pup=true")).filter(filterImpr(_))
    println(s"impr requests with pup: ${pupImprJsonRDD.count()}")
    
    val pupImprNoWsRDD = pupImprJsonRDD.filter(filterUserAgentForNoWs)
    println(s"impr requests with pup but no ws: ${pupImprNoWsRDD.count()}")
    
    val userAgentSingelCountRDD = pupImprNoWsRDD.map(a => (a.ua.getOrElse("no user agent"), 1))
    val userAgentCountRDD = userAgentSingelCountRDD.reduceByKey((a, b) => (a + b)).sortBy(c => c._2, false)
    userAgentCountRDD.foreach(a => println(s"${a._2} - ${a._1}"))
  }

  private def filterImpr(log: IhAccessLog): Boolean = {
    log.path match {
      case Some(p) if p.equals("/impr") => true
      case _ => false
    }
  }
  
  private def filterUserAgentForNoWs(log: IhAccessLog): Boolean = {
    val queryParams = log.query.split("&")
    val wsParam = queryParams.filter(_.startsWith("ws"))
    wsParam match {
      case p if p.length ==1 => {
        val ws = p.head.split("=")
        ws match {
          case x if x.length == 2 => {
            if (x(1).contains("adscale.de")) println(log)
            false}
          case _ => true
        }
      }
      case _ => true
    }
  }

  private def writeResultFile(responseResultFile: String, array: Array[String]) {
    val writer = new PrintWriter(new File(responseResultFile))
    array.foreach(entry => writer.write(s"${entry}\n"))
    writer.flush()
    writer.close()
  }
}