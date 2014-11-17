package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object FirstHitLogger {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("First Hit Logger").setMaster("local").setAppName("First Hit Logger File Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    val fileRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/sc-2651/ibid_first_hit-436--2014-11-16--*.csv.gz", 2)
    
    val filteredSlotsRDD = fileRDD.filter(line => line.contains("sid=NTM1ZjAw") || line.contains("sid=NTMwNzAw"))
    println(filteredSlotsRDD.count)
    filteredSlotsRDD.persist(StorageLevel.MEMORY_AND_DISK)
    
    val ptRDD = filteredSlotsRDD.filter(_.contains("pt="))
    println(ptRDD.count)
    ptRDD.top(10).foreach(println)
    
    val piRDD = filteredSlotsRDD.filter(_.contains("pi="))
    println(piRDD.count)
    
    sparkContext.stop
  }
}