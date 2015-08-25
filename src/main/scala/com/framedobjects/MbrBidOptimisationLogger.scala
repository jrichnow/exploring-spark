package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MbrBidOptimisationLogger {
  
  def main(args: Array[String]) {
    
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/pu"
    val logFileName = s"$investigationRootFolder/log/opt_requests-401.log"
    
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
        val jsonRDD = sparkContext.textFile(logFileName)
    println(jsonRDD.count)

  }
  
}