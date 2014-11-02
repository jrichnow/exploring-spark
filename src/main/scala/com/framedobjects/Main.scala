package com.framedobjects

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]) {
	  val sparkConfig = new SparkConf().setAppName("Test").setMaster("local")
	  val sparkContext = new SparkContext(sparkConfig)
	  val file = sparkContext.textFile("/users/jensr/Applications/spark-1.0.2-bin-hadoop2/README.md", 2)
	  
	  println(file.count)
	  println(file.filter(_.contains("Spark")).count)
  }
}