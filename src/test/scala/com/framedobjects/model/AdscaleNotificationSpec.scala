package com.framedobjects.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class AdscaleNotificationSpec extends FlatSpec with Matchers {

  "A loss notification log entry" should "be converted to a model" in {
    val log = """iid=40111144115514172401T040&uid=430231419926820355&tpuid=c10417d6-b4c5-401a-b813-aefa9d9bbde8&t=67&cid=4259856&rid=55e6484591f547e0e79355453ac6f8c0&win=false&p=0.19&r=cpm"""

    val model = AdscaleNotification.fromLog(log)
    model.iid should be("40111144115514172401T040")
    model.uid should be("430231419926820355")
    model.tpuid should be("c10417d6-b4c5-401a-b813-aefa9d9bbde8")
    model.t should be("67")
    model.cid.get should be("4259856")
    model.rid.get should be("55e6484591f547e0e79355453ac6f8c0")
    model.win should be(false)
    model.price should be(0.19d)
    model.r.get should be("cpm")
  }

  "A win notification log entry" should "be converted to a model" in {
    val log = """iid=40111144115514172401T040&uid=430231419926820355&tpuid=c10417d6-b4c5-401a-b813-aefa9d9bbde8&t=67&cid=4259856&rid=55e6484591f547e0e79355453ac6f8c0&win=true&p=0.19&"""

    val model = AdscaleNotification.fromLog(log)
    model.iid should be("40111144115514172401T040")
    model.uid should be("430231419926820355")
    model.tpuid should be("c10417d6-b4c5-401a-b813-aefa9d9bbde8")
    model.t should be("67")
    model.cid.get should be("4259856")
    model.rid.get should be("55e6484591f547e0e79355453ac6f8c0")
    model.win should be(true)
    model.price should be(0.19d)
  }
}