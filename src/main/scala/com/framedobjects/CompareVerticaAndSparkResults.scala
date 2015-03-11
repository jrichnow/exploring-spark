package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File

object CompareVerticaAndSparkResults {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
    val investigationRoot = "/users/jensr/Documents/DevNotes/investigations/sc-2666/15012015"
    val campaignId = 48520
    val handlerId = 435
    val fileNameIdentifiers = s"${campaignId}_${handlerId}_20_21"
    
    val iidsSparkFile = s"${investigationRoot}/${campaignId}/spark_${fileNameIdentifiers}-win-notification-iids.txt"
    val iidsVerticaFile = s"${investigationRoot}/${campaignId}/vert_${fileNameIdentifiers}-iids.txt"
      
    val iidSparkRDD = sparkContext.textFile(iidsSparkFile)
    println("spark: " + iidSparkRDD.count)
    
    val iidVerticaRDD = sparkContext.textFile(iidsVerticaFile)
    println("vertica: " + iidVerticaRDD.count)
    
//    val uniqueIidRDD = iidVerticaRDD.subtract(iidSparkRDD)
    val uniqueIidRDD = iidSparkRDD.subtract(iidVerticaRDD)
    println("unique: " + uniqueIidRDD.count)
    
    val fileName = s"${investigationRoot}/${campaignId}/spark_${fileNameIdentifiers}_delta-iids.txt"
    val writer = new PrintWriter(new File(fileName))
    uniqueIidRDD.toArray.foreach(entry => writer.println(entry))
    writer.flush()
    writer.close()
    
    println(uniqueIidRDD.top(20).toArray.mkString(","))
    
    sparkContext.stop
  }
}