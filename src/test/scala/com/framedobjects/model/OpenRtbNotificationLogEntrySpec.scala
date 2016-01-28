package com.framedobjects.model

import org.scalatest.{Matchers, FlatSpec}

class OpenRtbNotificationLogEntrySpec  extends FlatSpec with Matchers {

  "loss notification with deal ID" should "" in  {
    val log = """{"time":1453913894141,"tpid":66,"notifications":{"notifications":[{"id":"41897145391389411799T066","impid":"1","userid":"419151436988990753","buyeruid":"586C090C5CAE4373874E89C8CAC4DB69","bidid":"1","dealid":"la1n7sgywz","seat":"598785","crid":"671fb6aa-cfee-4a04-b4bf-86976144e15b-300x250-0-2-2","win":false,"p":0.01,"r":"floorInconsistency","t":21}]}}"""

    val notif = OpenRtbNotificationLogEntry.fromJson(log)

    notif.time should be (1453913894141l)
    notif.tpid should be (66)
    val notification = notif.notifications.notifications.head

    notification.id should be ("41897145391389411799T066")
    notification.win should be (false)
    notification.r.get should be ("floorInconsistency")
    notification.dealid.get should be ("la1n7sgywz")
  }
}
