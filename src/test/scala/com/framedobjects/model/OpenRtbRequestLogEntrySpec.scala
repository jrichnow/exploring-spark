package com.framedobjects.model

import org.scalatest.{Matchers, FlatSpec}

class OpenRtbRequestLogEntrySpec extends FlatSpec with Matchers  {

  "" should "" in {
    val log = """{"time":1453884660412,"tpid":38,"request":{"id":"41826145388466041001T038","imp":[{"id":"1","banner":{"w":800,"h":250,"pos":1,"topframe":0},"pmp":{"private_auction":0,"deals":[{"id":"exzp5vsnfy","bidfloor":4.15,"bidfloorcur":"EUR","at":2},{"id":"agizyyq6ae","bidfloor":1.5,"bidfloorcur":"EUR","at":2},{"id":"9xlag0hcow","bidfloor":5.21,"bidfloorcur":"EUR","at":2},{"id":"wjm2wvrrmt","bidfloor":5.21,"bidfloorcur":"EUR","at":2},{"id":"k04dkyzguy","bidfloor":4.15,"bidfloorcur":"EUR","at":2},{"id":"hj6ywnboqd","bidfloor":5.21,"bidfloorcur":"EUR","at":2}]},"tagid":"105691","ext":{"pv":0},"secure":0}],"site":{"id":"21474","domain":"hurriyet.com.tr","ref":"http://kelebekgaleri.hurriyet.com.tr/galeridetay/47817/2369/144/unlulerin-cocuklugu","page":"http://kelebekgaleri.hurriyet.com.tr/galeridetay/47817/2369/145/unlulerin-cocuklugu","publisher":{"id":"4164"}},"device":{"ip":"195.88.117.25","language":"de","ua":"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"},"user":{"id":"406811453878808604","buyeruid":"CAESEKGNdu4DvRCaLw0MPwk6sBU","ext":{}},"tmax":100}}"""

    val request = OpenRtbRequestLogEntry.fromJson(log)

    request.time should be (1453884660412l)
    request.request.id should be ("41826145388466041001T038")
    request.request.impressionId should be ("418261453884660410")
    request.request.imp.size should be (1)
    request.request.imp.head.tagid should be ("105691")

    val deals = request.request.imp.head.pmp.get.deals
    deals.size should be (6)

    val deal = deals.filter(_.id == "agizyyq6ae")
    deal.head.bidfloor should be (1.5)

    request.request.site.id should be ("21474")
    request.request.site.domain.get should be ("hurriyet.com.tr")
    request.request.site.ref.get should be ("http://kelebekgaleri.hurriyet.com.tr/galeridetay/47817/2369/144/unlulerin-cocuklugu")
    request.request.site.page.get should be ("http://kelebekgaleri.hurriyet.com.tr/galeridetay/47817/2369/145/unlulerin-cocuklugu")
  }
}
