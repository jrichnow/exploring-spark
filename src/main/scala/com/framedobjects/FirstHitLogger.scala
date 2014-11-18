package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import com.framedobjects.model.RequestLog
import java.io.PrintWriter
import java.io.File

object FirstHitLogger {
  
  val inputFiles = "/users/jensr/Documents/DevNotes/investigations/sc-2651/ibid_first_hit-436--2014-11-17--*.csv.gz"
  val outputFile = "/users/jensr/Documents/DevNotes/investigations/sc-2651/stroer-pt-2014-11-17.txt"

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("First Hit Logger").setMaster("local").setAppName("First Hit Logger File Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    val fileRDD = sparkContext.textFile(inputFiles, 2)
    
    val filteredSlotsRDD = fileRDD.filter(line => line.contains("sid=NTM1ZjAw") || line.contains("sid=NTMwNzAw"))
    println(filteredSlotsRDD.count)
    filteredSlotsRDD.persist(StorageLevel.MEMORY_AND_DISK)
    
    val ptRDD = filteredSlotsRDD.filter(_.contains("pt="))
    println(ptRDD.count)
    val requestPtRDD = ptRDD.map(entry => RequestLog.toRequestLog(entry))
    val requestUrlPrRDD =  requestPtRDD.map(request => request.slotId + "\t" + request.referrerUrl + "\t" + request.requestUri + "/" + request.queryString)
//    requestUrlPrRDD.foreach(println)
    
    val result = requestUrlPrRDD.toArray.mkString("\n")
    
    val writer = new PrintWriter(new File(outputFile))
    writer.write("slotId\treferrerUrl\trequestUri")
    writer.write(result)
    writer.flush
    writer.close
    
    val piRDD = filteredSlotsRDD.filter(_.contains("pi="))
    println(piRDD.count)
    
    sparkContext.stop
  }
}