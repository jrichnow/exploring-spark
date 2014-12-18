package com.framedobjects

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.framedobjects.model.OpenRtbResponseLogEntry
import com.framedobjects.model.OpenRtbRequestLogEntry

class OpenRtbLogFileAnalyserSpec extends FlatSpec with Matchers {

  "A JSON request string" should "convert to model" in {
    val jsonString = """
      {"time":1418112543570,"tpid":48,"request":{"id":"419271418112543552","imp":[{"id":"1","banner":{"w":300,"h":600,"pos":1,"topframe":1},"tagid":"105175","ext":{"pv":0},"bidfloor":0}],"site":{"id":"29615","ref":"http://www.markt.de/benutzer/","page":"http://www.markt.de/meinmarkt.htm","publisher":{"id":"13002"}},"device":{"ip":"192.109.190.88","ua":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36","language":"de"},"user":{"id":"419361418112520286","buyeruid":"5318aaf47183453de1072bd9dc35b02a"},"tmax":60000}}
      """

    val request = OpenRtbRequestLogEntry.fromJson(jsonString)
    println(request)
    
    request.time should be (1418112543570l)
    request.tpid should be (48)
    request.request.id should be ("419271418112543552")
    request.request.imp(0).id should be ("1")
    request.request.imp(0).tagid should be ("105175")
    request.request.site.id should be ("29615")
    request.request.site.publisher.id should be ("13002")
    request.request.user.id should be ("419361418112520286")
  }

  "A JSON response string" should "convert to model" in {
    val jsonString = """
	    {"time":1418114936743,"tpid":48,"responseTime":41,"response":{"id":"414311418114936699","seatbid":[{"bid":[{"adm":"<iframe src=\"//tracking.m6r.eu/sync/creative?creativeId=${creative.name#urlencode}&id=adscale-auction%3A${bidrequest.id}&mbrUserId=${meta.mbrUserId}&z=\" width=\"${creative.width}\" height=\"${creative.height}\" frameborder=\"0\" scrolling=\"no\"><\/iframe>","adomain":["bonprix.ch"],"cid":"bonprix.de/nkp","crid":"bonprix.de/nkp_ch/160x600_fix","ext":{"agn":"mbr-for-adscale","avn":"bonprix.de"},"id":"414311418114936699~1","impid":"1","price":0.066549}],"seat":"mbr-for-adscale"}]}}
	    """
    val response = OpenRtbResponseLogEntry.fromJson(jsonString)
    println(response)

    response.time should be(1418114936743l)
    response.tpid should be(48)
    response.responseTime should be(41)
    response.response.id should be("414311418114936699")
    response.response.seatbid(0).bid(0).id should be("414311418114936699~1")
    response.response.seatbid(0).bid(0).impid should be("1")
    response.response.seatbid(0).bid(0).price should be(0.066549)
  }

  "An earlier time" should "be before a later time" in {
    val startDateTime = new DateTime(2014, 12, 9, 9, 0, 0, DateTimeZone.UTC)

    OpenRtbLogFileAnalyser.isBefore(1418114936743l, startDateTime) should be(true)
    OpenRtbLogFileAnalyser.isBefore(1418116646819l, startDateTime) should be(false)

    OpenRtbLogFileAnalyser.isAfterIncluding(1418114936743l, startDateTime) should be(false)
    OpenRtbLogFileAnalyser.isAfterIncluding(1418116646819l, startDateTime) should be(true)
    
    
    println(new DateTime(1418805540400l, DateTimeZone.UTC))
  }
}