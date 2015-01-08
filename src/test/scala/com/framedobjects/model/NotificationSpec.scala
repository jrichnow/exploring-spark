package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class NotificationSpec  extends FlatSpec with Matchers {

  "A log line" should "convert into a Notification object" in {
    val logEntry = "2015-01-08 02:05:38,901|Error sending notification BidOptimisationWinLossNotification [impressionId=334671420682736897, userId=334651420682736897, winningPrice=0, win=false, reason=OTHER, winningAdvertId=0, tpuid=null, attempt=false] to http://notifying.eu1.m6r.eu:9062/adscale-ecpm/notify?"
      
    val notif = Notification.fromLogEntry(logEntry)
    println(notif)
    notif.impressionId should be ("334671420682736897")
  } 
}