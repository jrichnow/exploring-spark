package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.framedobjects.model.IhAccessLog
import org.apache.spark.rdd.RDD

object IhAccessLogger {

  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1147"
    val logFileName = s"$investigationRootFolder/logs/ih-access-40*.log"

    val slotIds = Seq("NGNjYjgw")

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val jsonRDD = sparkContext.textFile(logFileName).filter(filterSlot(_)).map(mapToJson(_))
    println(jsonRDD.count)

    val keyedByIidRDD = jsonRDD.map(keyByIid(_))
    println(keyedByIidRDD.count)

    val groupedByIidRDD = keyedByIidRDD.groupByKey
    println(groupedByIidRDD.count)
    
    val filteredMissingWsRDD = groupedByIidRDD.filter(filterEmptyWsParam)
    println(filteredMissingWsRDD.count)

    for ((iid, logEntries) <- filteredMissingWsRDD) {
      if (logEntries.size >= 5) {
        println(s"$iid")
        println(s"has ws: ${hasWsParam(logEntries.toSeq)}, has ws value: ${hasWsValue(logEntries.toSeq)}")
        logEntries.foreach(l => println(s"\t$l"))
      }
    }

  }

  private def filterSlot(line: String): Boolean = {
    line.contains("NGNjYjgw")
  }

  private def mapToJson(line: String): IhAccessLog = {
    IhAccessLog.fromJson(line)
  }

  private def keyByIid(log: IhAccessLog): (String, IhAccessLog) = {
    log.data match {
      case Some(d) => (d.iid, log)
      case None => {
        val iid = log.query.split("&")(1).split("=")(1)
        (iid, log)
      }
    }
  }
  
  private def filterEmptyWsParam(log: (String, Iterable[IhAccessLog])): Boolean = {
    hasWsParam(log._2.toSeq) && !hasWsValue(log._2.toSeq)
  }
  
  private def hasWsParam(logs: Seq[IhAccessLog]):Boolean = {
    !logs.filter(_.query.contains("&ws=")).isEmpty
  }
  
  private def hasWsValue(logs: Seq[IhAccessLog]): Boolean = {
    logs.filter(_.hasWsParamValue).size > 0
  }
  
  private def hasRefererHeader(logs: Seq[IhAccessLog]):Boolean = {
	  !logs.filter(_.ref.isDefined).isEmpty
  }
}