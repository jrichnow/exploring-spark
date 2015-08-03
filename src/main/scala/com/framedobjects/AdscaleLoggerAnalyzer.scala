package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.AdscaleResponseLogEntry

object AdscaleLoggerAnalyzer {

  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/nextperformance"
    val responseHttpFileName = s"$investigationRootFolder/logs/dsp-adscale-bidresponse-434.log"

    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)

    val rawResponseRDD = sparkContext.textFile(responseHttpFileName);
    val filteredResponseRDD = rawResponseRDD.filter(filterByPartnerId(_, "36"))
    filteredResponseRDD.foreach(println)

//    val mappedResponseRDD = filteredResponseRDD.map(line => AdscaleResponseLogEntry.fromJson(line.split(", ")(3).trim()))
//    mappedResponseRDD.foreach(println)
  }

  private def filterByPartnerId(line: String, id: String): Boolean = {
    val lineArray = line.split(", ")
    lineArray(1).trim() match {
      case `id` => true
      case _ => false
    }
  }
}