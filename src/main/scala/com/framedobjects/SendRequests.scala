package com.framedobjects

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io._
import java.net.URL

object SendRequests {
  val r = scala.util.Random

  def main(args: Array[String]) {
    (1 to 100000).foreach(send(_))
  }

  private def send(x: Int) {
    val currentTime = System.currentTimeMillis()
    for (i <- 0 to 9) {
      val creationTime = currentTime - 400 - r.nextInt(200)
      val offset = r.nextInt(300)
      val partnerId = 990 + i
      val url = s"http://localctl.test:8080/adserver-ih/tpui/400381438042202155/$creationTime/$offset?tpid=$partnerId&tpuid=10000088"
      Source.fromURL(new URL(url))
    }
    if (x % 100 == 0) {
    	println(s"sent ${x * 10} requests")
    }
  }
}