package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.CounterRecord

object ClickInvestigation {
  
  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1337"
    val counterFile = s"$investigationRootFolder/logs/GTR-counters-slots-86088-86087-86095-20151001-20151007"
    
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Click Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
    val counterTextRDD = sparkContext.textFile(counterFile)
    val counterRDD = counterTextRDD.map(mapToCounter(_))
    val filteredRDD = counterRDD.filter(filterClicks)
    
    println("============ Filtered")
    filteredRDD.foreach(println)
    
    println(filteredRDD.count())
  }
  
  private def filterClicks(counter: CounterRecord): Boolean = {
    Seq(0,2,4,9,30,31).contains(counter.typeId) && counter.cost != "0"
  }
  
  private def mapToCounter(record: String): CounterRecord = {
    CounterRecord.fromRecord(record)
  }
}