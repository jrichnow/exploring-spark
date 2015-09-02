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
  val date = "2015-09-02" 
  val handler = "01"
  
  val destinationFolder = s"/users/jensr/Documents/DevNotes/investigations/adscale-1213/logs/$date/"

  def main(args: Array[String]) {
    val ih = 4
    val dh = 3
    val ihInstances = List(14, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36)
    val dhInstances = List(20, 21, 26, 27, 29, 30, 31, 32, 33, 34)

    val apps = Map(ih -> ihInstances, dh -> dhInstances)
    
//    getLogFiles("dsp-adscale-bidrequest-", (0 to 24).toList)
//    getLogFiles("dsp-adscale-bidresponse-", (0 to 18).toList)
//    getLogFiles("dsp-openrtb-bidrequest-", (0 to 17).toList)
//    getLogFiles("dsp-openrtb-bidresponse-", (0 to 13).toList)
  }
  
  private def getLogFiles(fileRoot: String, fileParts: List[Int]) {
    for (file <- fileParts) {
      val fileName = s"adscale@app$handler:$sourceRoot/${fileRoot}4${handler}--$date--$file.log.gz"
      transferFile(fileName, destinationFolder)
    }
    val fileRootRegex = fileRoot.r
    while (!checkFileCount(destinationFolder, fileRootRegex, fileParts.length)) {
      Thread.sleep(1000)
    }
  }
  
  private def checkFileCount(destinationFolder:String, fileRegex: Regex, expectedNumberOfFiles: Int): Boolean = {
    val result = s"ls -l $destinationFolder"!!
    
    fileRegex.findAllIn(result).length == expectedNumberOfFiles
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