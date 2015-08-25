package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class RhAccessLogSpec extends FlatSpec with Matchers {

  "A entry in the rh-access log file" should "be parsed correctly" in {
    val logEntry = """2015-08-22 00:00:00.000|||/rh/14424/adscale|||https://api.zanox.ws/xhtml/2011-03-01/applications/iframe/E4EACC04FA19F8618F23?mediaslot=270B072F2547EB5BFCF2|||Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0|||407251307626016651"""

    val log = RhAccessLog.fromLog(logEntry)

    log.time should be ("2015-08-22 00:00:00.000")
    log.pixelId should be ("/rh/14424/adscale")
    log.referrer should be ("https://api.zanox.ws/xhtml/2011-03-01/applications/iframe/E4EACC04FA19F8618F23?mediaslot=270B072F2547EB5BFCF2")
    log.ua should be ("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0")
    log.impressionId should be ("407251307626016651")
  }
}