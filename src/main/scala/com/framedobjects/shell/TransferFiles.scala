package com.framedobjects.shell

import sys.process._
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Future

object TransferFiles {

  def main(args: Array[String]) {
    val ih = 4
    val dh = 3
    val ihInstances = List(14, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36)
    val dhInstances = List(20, 21, 26, 27, 29, 30, 31, 32, 33, 34)

    val apps = Map(ih -> ihInstances, dh -> dhInstances)

    val startTime = System.currentTimeMillis()

    for ((handler, instances) <- apps) {
      val result = instances.map(id => future { transferFileForAppInstance(id, handler) })
      result.foreach(r => {
        while (!r.isCompleted) {
          Thread.sleep(1000)
        }
      })
    }

    println(s"All done! It took ${(System.currentTimeMillis() - startTime) / 1000} seconds")
  }

  private def transferFileForAppInstance(instanceId: Int, handlerType: Int): String = {
    val sourceFile = s"adscale@app$instanceId://data/adscale/mbr-log/opt_notif-error-$handlerType$instanceId.log"
    val destinationFolder = "/users/jensr/Documents/DevNotes/investigations/sc-2666/08012015/"

    println(s"app$handlerType$instanceId: starting download of $sourceFile")
    val result = s"scp $sourceFile $destinationFolder" !!

    println(s"app$handlerType$instanceId: file transfer complete with result: '$result'")

    result
  }
}