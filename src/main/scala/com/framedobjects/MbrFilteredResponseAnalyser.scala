package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.framedobjects.model.OpenRtbResponseLogEntry

object MbrFilteredResponseAnalyser {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
     val rawResponsesRDD = sparkContext.textFile("/users/jensr/Documents/DevNotes/investigations/adscale-538a/responses.log", 2)
     
     val auction22_RDD = rawResponsesRDD.filter(_.contains("o3yasaq4ih"))
//     auction22_RDD.foreach(println)
     
     val price025RDD = auction22_RDD.filter(_.contains("\"price\":1"))
     val impressionIds025 = price025RDD.map(OpenRtbResponseLogEntry.fromJson(_)).map(_.response.id).map(_.substring(0, 18))
     println(impressionIds025.toArray.mkString(","))
  }
}