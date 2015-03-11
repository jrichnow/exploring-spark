package com.framedobjects

import java.io.File

object S3AllAppFileDownloader {

  private val outputFolder = "/users/jensr/Documents/DevNotes/investigations/sc-2666/15012015/logs"
  private val bucketName = "rtb-archive"

  def main(args: Array[String]) {
    val ih = 4
    val dh = 3
    val ihInstances = List(14, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36)
    val dhInstances = List(20, 21, 26, 27, 29, 30, 31, 32, 33, 34)

    val apps = Map(ih -> ihInstances, dh -> dhInstances)

    val fileParts = List(0, 1, 2)

    val startTime = System.currentTimeMillis()
    
    transferFiles(apps, fileParts)
    
    println(s"All done! It took ${(System.currentTimeMillis() - startTime) / 1000} seconds")
  }

  private def transferFiles(apps: Map[Int, List[Int]], fileParts: List[Int]) {
    for ((handler, instances) <- apps) {
      instances.map(id => transferFilesForApp(handler, id, fileParts))
    }
  }

  private def transferFilesForApp(handlerType: Int, instanceId: Int, fileParts: List[Int]) {
    fileParts.foreach(transferFilePart(handlerType, instanceId, _))
  }

  private def transferFilePart(handlerType: Int, instanceId: Int, filePart: Int) {
//    val s3File = s"mbr_responses/20150114/opt_responses-${handlerType}${instanceId}--2015-01-14--${filePart}.log.gz"
    val s3File = s"mbr_notifications/20150115/opt_notif-${handlerType}${instanceId}--2015-01-15--${filePart}.log.gz"
    println(s3File)

    val fileName = s3File.split("/")(2)
    val outputFile = new File(s"${outputFolder}/${fileName}")

    S3FileTransfer.transferFile(bucketName, s3File, outputFile)
  }
}