package com.framedobjects

import java.io.File

object S3AllAppIhAccessFileDownloader {

  private val outputFolder = "/users/jensr/Documents/DevNotes/investigations/adscale-1147/logs/20150815"
  private val bucketName = "rtb-archive"

  def main(args: Array[String]) {
    val ih = 4
//    val ihInstances = List("02", "03","04", "05", "06", "07", "08", "09", "10", "11", "12", "18", "19", "20", "21")
    val ihInstances = List("01")

    val apps = Map(ih -> ihInstances)

//    val fileParts = List(0, 1, 2, 3, 4)
    val fileParts = (5 to 91).toList

    val startTime = System.currentTimeMillis()
    
    transferFiles(apps, fileParts)
    
    println(s"All done! It took ${(System.currentTimeMillis() - startTime) / 1000} seconds")
  }

  private def transferFiles(apps: Map[Int, List[String]], fileParts: List[Int]) {
    for ((handler, instances) <- apps) {
      instances.map(id => transferFilesForApp(handler, id, fileParts))
    }
  }

  private def transferFilesForApp(handlerType: Int, instanceId: String, fileParts: List[Int]) {
    fileParts.foreach(transferFilePart(handlerType, instanceId, _))
  }

  private def transferFilePart(handlerType: Int, instanceId: String, filePart: Int) {
    val s3File = s"ih_access/20150815/ih-access-${handlerType}${instanceId}--2015-08-15--${filePart}.log.gz"
    println(s3File)

    val fileName = s3File.split("/")(2)
    val outputFile = new File(s"${outputFolder}/${fileName}")

    S3FileTransfer.transferFile(bucketName, s3File, outputFile)
  }
}