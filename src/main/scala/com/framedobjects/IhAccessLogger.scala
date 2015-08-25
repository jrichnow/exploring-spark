package com.framedobjects

import java.io.File
import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.framedobjects.model.IhAccessLog

object IhAccessLogger {

//  val slotId = "NDdiMDA" //73408
  	val slotId = "NGNjYjgw" //78638
  //  val slotId = "NzFkODgw" //116578

  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/pu"
    val logFileName = s"$investigationRootFolder/log/ih-access-401.log"

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val jsonRDD = sparkContext.textFile(logFileName).filter(filterSlot(_)).map(mapToJson(_))
    println(jsonRDD.count)

    val keyedByIidRDD = jsonRDD.map(keyByIid(_))
    println(keyedByIidRDD.count)

    val groupedByIidRDD = keyedByIidRDD.groupByKey
    println(groupedByIidRDD.count)

    val fullSequenceRDD = groupedByIidRDD.filter(_._2.size >= 5)
    println(s"Full sequence >= 5: ${fullSequenceRDD.count}")

    val filteredMissingWsRDD = groupedByIidRDD.filter(filterEmptyWsParam)
//    filteredMissingWsRDD.foreach(println)
    println(s"Missing ws value and >= 5: ${filteredMissingWsRDD.count}")

    writeResultFile(s"$investigationRootFolder/$slotId.txt", filteredMissingWsRDD.toArray())
    val userAgentRDD = filteredMissingWsRDD.map(mapUserAgentSimple(_))
    val userAgentCount = userAgentRDD.count()
    println(s"Number of user agent points: ${userAgentCount}")

    val userAgentCountRDD = userAgentRDD.reduceByKey((a, b) => (a + b)).sortBy(c => c._2, false)

    println(s"user agent breakdown for slot: $slotId")
    userAgentCountRDD.foreach(a => println(s"${a._2} = ${(a._2.toDouble * 100 / userAgentCount).toFloat}% - ${a._1}"))
  }

  private def filterSlot(line: String): Boolean = {
    line.contains(slotId)
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

  private def mapUserAgentFull(log: (String, Iterable[IhAccessLog])): (String, Integer) = {
    (log._2.head.ua.getOrElse("no user agent"), 1)
  }

  private def mapUserAgentSimple(log: (String, Iterable[IhAccessLog])): (String, Integer) = {
    val ua = log._2.head.ua
    ua match {
      case Some(a) if a.contains("iPad;") => ("iPad", 1)
      case Some(b) if b.contains("iPhone;") => ("iPhone", 1)
      case Some(c) if c.contains("Macintosh;") => ("Macintosh", 1)
      case Some(d) if d.contains("Linux;") => ("Android", 1)
      case Some(e) if e.contains("BB10;") => ("BB10", 1)
      case Some(f) if f.contains("X11;") => ("X11", 1)
      case Some(g) if g.contains("PlayStation") => ("PlayStation", 1)
      case Some(h) if h.contains("Windows NT") => ("Windows", 1)
      case Some(x) => ("other user agents", 1)
      case None => ("no user agent", 1)
    }
  }

  private def hasWsParam(logs: Seq[IhAccessLog]): Boolean = {
    !logs.filter(_.query.contains("&ws=")).isEmpty
  }

  private def hasWsValue(logs: Seq[IhAccessLog]): Boolean = {
    logs.filter(_.hasWsParamValue).size > 0
  }

  private def hasRefererHeader(logs: Seq[IhAccessLog]): Boolean = {
    !logs.filter(_.ref.isDefined).isEmpty
  }

  private def writeResultFile(responseResultFile: String, rdd: Array[(String, Iterable[IhAccessLog])]) {
    val writer = new PrintWriter(new File(responseResultFile))
    for ((iid, logEntries) <- rdd) {
      if (logEntries.size >= 5) {
        writer.write(s"$iid\thas ws:  ${hasWsParam(logEntries.toSeq)}, has ws value: ${hasWsValue(logEntries.toSeq)}\n")
        logEntries.foreach(l => writer.write(s"\t$l\n"))
        println(s"\t${logEntries.head.ua}\n")
      }
      writer.write("\n")
    }
    writer.flush()
    writer.close()
  }
}