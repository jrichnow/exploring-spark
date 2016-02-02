package com.framedobjects.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object SqlTest {

  def main(args: Array[String]) {
    val investigationRootFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1674"
    val bidresponseFileName = s"$investigationRootFolder/logs/dsp-openrtb-bidresponse-418--2016-01-27--0.log.gz"

    val context = new SparkConf().setMaster("local").setAppName("SQL Tests")
    val sc = new SparkContext(context)

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json(bidresponseFileName)
    df.show(10)
    df. printSchema()

    df.registerTempTable("responses")
    val sqlQuery = sqlContext.sql("select * from responses limit 10")
    println(sqlQuery.show(10))

    sqlContext.sql("select max(tpid) as maxTime, min(tpid) as minTime from responses").show()

    sqlContext.sql("select tpid, max(responseTime) as maxTime, min(responseTime) as minTime from responses group by tpid").show()
  }
}
