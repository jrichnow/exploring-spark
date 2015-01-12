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
    
    val iidsSparkFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/48508_winning-iids-20-21.txt"
    val iidsVerticaFile = "/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/vert-48508-09012015-20-21.txt"
      
    val iidSparkRDD = sparkContext.textFile(iidsSparkFile)
    println("spark: " + iidSparkRDD.count)
    
    val iidVerticaRDD = sparkContext.textFile(iidsVerticaFile)
    println("vertica: " + iidVerticaRDD.count)
    
    val uniqueIidRDD = iidVerticaRDD.subtract(iidSparkRDD)
    println("unique: " + uniqueIidRDD.count)
    uniqueIidRDD.foreach(println)
    
    val fileName = s"/users/jensr/Documents/DevNotes/investigations/sc-2666/09012015/48508_delta-iids-20-21.txt"
    val writer = new PrintWriter(new File(fileName))
    uniqueIidRDD.toArray.foreach(entry => writer.println(entry))
    writer.flush()
    writer.close()
    
//    println(uniqueIidRDD.toArray.mkString(","))
    
    sparkContext.stop
  }
}