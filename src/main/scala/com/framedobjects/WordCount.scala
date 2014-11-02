package com.framedobjects

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {

  def main(args: Array[String]) {
	  val sparkConfig = new SparkConf().setAppName("Word Count").setMaster("local")
	  val sparkContext = new SparkContext(sparkConfig)
	  val file = sparkContext.textFile("/users/jensr/Applications/spark-1.0.2-bin-hadoop2/README.md", 2)
	  
	  // Split it up into words.
	  val words = file.flatMap(line => line.split(" "))
	  // Transform into word and count.
	  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
	  
	  counts.foreach(println)
	  
	  counts.saveAsTextFile("/Users/jensr/Temp/wordcount.txt")
	  
	  sparkContext.stop
  }
}