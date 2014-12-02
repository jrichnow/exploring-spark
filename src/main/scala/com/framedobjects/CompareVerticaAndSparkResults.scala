package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object CompareVerticaAndSparkResults {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Test").setMaster("local").setAppName("RTB Log Files Investigation")
    val sparkContext = new SparkContext(sparkConfig)
    
    val iidsSparkFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/25112014/38575-iids-20-21.txt"
    val iidsVerticaFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/25112014/vert-38575-20-21.txt"
      
    val iidSparkRDD = sparkContext.textFile(iidsSparkFile)
    println("spark: " + iidSparkRDD.count)
    
    val iidVerticaRDD = sparkContext.textFile(iidsVerticaFile).map(_.trim())
    println("vertica: " + iidVerticaRDD.count)
    
    val uniqueIidRDD = iidVerticaRDD.subtract(iidSparkRDD)
    println("unique: " + uniqueIidRDD.count)
  }
}