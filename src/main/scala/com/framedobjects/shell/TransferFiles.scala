package com.framedobjects.shell

import sys.process._
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Future
import scala.util.matching.Regex

object TransferFiles {
  
  val sourceRoot = "//data/adscale/dsp-log"
  val date = "2016-01-06"

  val destinationFolder = s"/users/jensr/Documents/DevNotes/investigations/adscale-1559/logs/$date/"

  def main(args: Array[String]) {
    val dhList = (1 to 8).toList.map(x => s"0$x")
    for (dh <- dhList) {
//      getGzLogFiles(dh, "dsp-adscale-bidresponse-", (0 to 7).toList)
//      getLogFiles(dh, "dsp-adscale-bidresponse-3")
      getLogFiles(dh, "dsp-adscale-notification-3")
    }

//    getLogFiles("dsp-adscale-bidresponse-", (0 to 18).toList)
//    getLogFiles("dsp-openrtb-bidrequest-", (0 to 17).toList)
//    getLogFiles("dsp-openrtb-bidresponse-", (0 to 13).toList)
  }

  private def getGzLogFiles(handler: String, fileRoot: String, fileParts: List[Int]) {
    for (file <- fileParts) {
      val fileName = s"adscale@app$handler:$sourceRoot/${fileRoot}${handler}--$date--$file.log.gz"
      transferFile(fileName, destinationFolder)
    }
    val fileRootRegex = fileRoot.r
    while (!checkFileCount(destinationFolder, fileRootRegex, fileParts.length)) {
      Thread.sleep(1000)
    }
  }

  private def getLogFiles(handler: String, fileRoot: String) {
    val fileName = s"adscale@app$handler:$sourceRoot/${fileRoot}${handler}.log"
    transferFile(fileName, destinationFolder)

    while (!checkFileHasTransferred(destinationFolder, s"${fileRoot}${handler}.log")) {
      Thread.sleep(1000)
    }
  }
  
  private def checkFileCount(destinationFolder:String, fileRegex: Regex, expectedNumberOfFiles: Int): Boolean = {
    val result = s"ls -l $destinationFolder"!!

    fileRegex.findAllIn(result).length == expectedNumberOfFiles
  }

  private def checkFileHasTransferred(destinationFolder:String, fileName: String): Boolean = {
    val result = s"ls -l $destinationFolder"!!

    result.contains(fileName)
  }
  
  private def transferFile(sourceFileName: String, destinationFolder: String):Future[String] = Future {
    println(s"transfering $sourceFileName ...")
	  val result = s"scp $sourceFileName $destinationFolder" !!
    
    println(s"... $sourceFileName transfer done with result of $result")
    result
  }

  private def transferErrorNotifFilesFromApps(apps: Map[Int, List[Int]]) {
    for ((handler, instances) <- apps) {
      val resultFutureList = instances.map(id => future { transferErrorNotifFileForAppInstance(id, handler) })
      resultFutureList.foreach(r => {
        while (!r.isCompleted) {
          Thread.sleep(1000)
        }
      })
    }
  }
  
  private def transferErrorNotifFileForAppInstance(instanceId: Int, handlerType: Int): String = {
    val sourceFile = s"adscale@app$instanceId://data/adscale/mbr-log/opt_notif-error-$handlerType$instanceId.log"
    val destinationFolder = "/users/jensr/Documents/DevNotes/investigations/sc-2666/08012015/"

    println(s"app$handlerType$instanceId: starting download of $sourceFile")
    val result = s"scp $sourceFile $destinationFolder" !!

    println(s"app$handlerType$instanceId: file transfer complete with result: '$result'")
    result
  }
}