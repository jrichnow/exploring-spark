package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.framedobjects.model.IhAccessLog

object SimpleIhAccessLogger {

  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1182/"
    val logFileName = s"$investigationRootFolder/logs/20150821/ih-access-401.log"

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val jsonRDD = sparkContext.textFile(logFileName).map(IhAccessLog.fromJson(_))
    println(s"total count: ${jsonRDD.count()}")

    val imprRDD = jsonRDD.filter(filterImprWithPup(_))
    println(s"total count impr with /pup: ${imprRDD.count()}")

    val wsRDD = imprRDD.filter(hasWsParam(_))
    println(s"total count impr with ws: ${wsRDD.count()}")

    val validWsRDD = wsRDD.filter(hasValidWsParam(_))
    println(s"total count impr with valid ws: ${validWsRDD.count()}")

    val notWsRDD = imprRDD.filter(!hasWsParam(_))
    println(s"total count impr with no ws: ${notWsRDD.count()}")
    notWsRDD.foreach(l => println(l.ua))
  }

  private def filterImprWithPup(log: IhAccessLog): Boolean = {
    log.path match {
      case Some(p) if p.equals("/impr") => {
        log.ref match {
          case Some(r) if (r.contains("/pup")) => true
          case _ => false
        }
      }
      case None => false
      case _ => false
    }
  }

  private def hasWsParam(log: IhAccessLog): Boolean = {
    log.query.contains("&ws=")
  }

  private def hasValidWsParam(log: IhAccessLog): Boolean = {
    val params = log.query.split("&")
    val wsParam = params.filter(_.startsWith("ws=")).head
    val wsArray = wsParam.split("=")
    if (wsArray.length == 2) {
      !wsArray(1).contains("adscale.de")
    } else {
      println(log.ua)
      false
    }
  }
}