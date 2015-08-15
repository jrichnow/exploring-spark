package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class IhAccessLogSpec extends FlatSpec with Matchers {

  "A /impr log line" should "convert to an IhAccessLog instance" in {

    val logEntry = """{"time":1439347517407,"path":"/impr","ref":"http://preiswerte-handy.com/content.php","ua":"Mozilla/5.0 (Windows NT 6.1; rv:37.0) Gecko/20100101 Firefox/37.0","query":"v=2&sid=NGZmNWMw&nu=0&t=1424925812399&iFrame&ssl=0&x=418181438974671927","ip":"92.206.234.219","data":{"uid":"418181438974671927","iid":"402451439347517407","iidx":"01","sid":81879,"iFrame":true,"newUser":false,"ts":0}}"""

    val imprLog = IhAccessLog.fromJson(logEntry)
    
    imprLog.time should be (1439347517407l)
    imprLog.path.get should be ("/impr")
    imprLog.ref.get should be ("http://preiswerte-handy.com/content.php")
    imprLog.ua should be ("Mozilla/5.0 (Windows NT 6.1; rv:37.0) Gecko/20100101 Firefox/37.0")
    imprLog.query should be ("v=2&sid=NGZmNWMw&nu=0&t=1424925812399&iFrame&ssl=0&x=418181438974671927")
    imprLog.ip should be ("92.206.234.219")
    
    val data = imprLog.data.get
    data.uid should be ("418181438974671927")
    data.iid should be ("402451439347517407")
    data.iidx should be ("01")
    data.sid should be (81879)
    data.iFrame should be (true)
    data.newUser should be (false)
  }

  "A /pup log line" should "convert to an IhAccessLog instance" in {

    val logEntry = """{"time":1439347536690,"ua":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12","query":"v=2&iid=410831438542404174&hid=410&sid=NWQ4YTQw&lb=node10.lb.adscale.de&nu=0&uu=405291438542300121&ssl=0&x=419941438542241096&ref=http://www.partner-hund.de/info-rat/urlaub/laenderbestimmungen/einreisebestimmungen.html&iidx=01&ws=partner-hund.de&pup=true","ip":"2.16.101.30"}"""

    val pupLog = IhAccessLog.fromJson(logEntry)
    println(pupLog)
    pupLog.time should be (1439347536690l)
    pupLog.path should be (None)
    pupLog.ref should be (None)
    pupLog.ua should be ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12")
    pupLog.query should be ("v=2&iid=410831438542404174&hid=410&sid=NWQ4YTQw&lb=node10.lb.adscale.de&nu=0&uu=405291438542300121&ssl=0&x=419941438542241096&ref=http://www.partner-hund.de/info-rat/urlaub/laenderbestimmungen/einreisebestimmungen.html&iidx=01&ws=partner-hund.de&pup=true")
    pupLog.ip should be ("2.16.101.30")
    pupLog.data should be (None)
    
    pupLog.query.split("&")(1).split("=")(1) should be ("410831438542404174")
  }
}