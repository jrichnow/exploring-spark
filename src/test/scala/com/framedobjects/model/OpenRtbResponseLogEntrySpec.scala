package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class OpenRtbResponseLogEntrySpec extends FlatSpec with Matchers {
  
  "" should "" in {
val log =
  """
    |{"time":1453884999892,"tpid":38,"requestId":"41831145388499984901T038","responseTime":40,"response":{"id":"41831145388499984901T038","seatbid":[{"bid":[{"id":"0","impid":"1","price":0.19548,"adid":"16478941","adm":"<div class=\"GoogleActiveViewClass\" id=\"DfaVisibilityIdentifier_3176437974\"><iframe src=\"https://www.ad4mat.de/ads/conbanner_bild1.php?cat=bild_bildbanner&w=728&h=90&anim=0&ibtn=0&zanox_tracking_host=https://ad.zanox.com/ppc/&zanox_tracking_param=36028457C805199353&cachebuster=ABAjH0itgmKXmbqolRgAHLnYdwlm&adclick=https://bid.g.doubleclick.net/xbbe/creative/click%3Fd%3DAPEucNXywpIyuTXsbLyrIwwC2KZDjXFO4u9ySriwlSR-s15OjqC9JHvwPRTsqFTYMhh3N6s4r8hWF5Ce4yZPQ389ao-8qwZNUA%26r1%3D\" frameborder=\"0\"  width=\"728\" height=\"90\" scrolling=\"no\"><\/iframe>\n<script type=\"text/javascript\">  if ('') {  document.write('<script src=\"https://c.betrad.com/surly.js?;ad_w=728;ad_h=90;coid=322;nid=4311;cps=\"><\/sc' + 'ript>');  }<\/script><iframe src=\"https://googleads.g.doubleclick.net/xbbe/pixel?d=CICHEhC30jMY3eXtBw&v=APEucNUrmWdbmPJUz12BUZfxTSGi-LA1yi9mp5b0OJ5xzpUnz5HB-T-fcp-B1YyEZQGLNTi8wyKyLEEk8Opl1tqpcsANyYW6oG6-4qZiJUwEqBuJDVUMGKA\" style=\"display:none\"><\/iframe><\/div><SCRIPT language='JavaScript1.1' SRC=\"https://bid.g.doubleclick.net/xbbe/creative/adj?d=APEucNUHrG67UuT2hXa1lbeDQyGtovQScAkkm7rRaDfNyjGxOuhyoAMtkKDRWfuoezy-4greADO1QvW7TGUd75rDijvavKrllJkgDgNaquOCAYNW9AU0CWBDwMkWbVC84o7iRnxiStQAzMVXitj5tsglwZq4bMnjL8-55N0SYiZyLspSafQ-y6L6Sjbr8LSwFsWisMTDW0bjFqwVa2oEgLPsbK0QZXQ-SEoIXcoVpPaOO9cXwNI_2uORCxsf9KS5JaFRKuuM2i-FwD7vpGbC7vFzAwnu5qSouqQzeRFgpgKG40V7dFVb6BslApYKdTluaqeAvrkojAxcBbEa1IplY36vImF2y-_j1pwp14YffX0HhehXZ9rX-PUz77GXg1lwRquNC2Atb18r0WKgorQ6aX52LmyM-xDnqg&pr=${AUCTION_PRICE:ENC}\"><\/SCRIPT><DIV STYLE=\"position: absolute; left: 0px; top: 0px; visibility: hidden;\"><IMG SRC=\"https://bid.g.doubleclick.net/xbbe/beacon?data=APEucNXywpIyuTXsbLyrIwwC2KZDfT9Ppv0CD9qcS-iH5BwtBYmtMD0z1_Bj5Ip0m_fX96o5o_ZzdlXlLFz_iFb2jgIIp4yO9Q\" BORDER=0 WIDTH=1 HEIGHT=1 ALT=\"\" STYLE=\"display:none\"><\/DIV>","adomain":["ad4mat.com"],"cid":"4644871","crid":"16478941","w":728,"h":90,"ext":{"avn":"DE - Reach","agn":"295808"}}],"seat":"295808"}],"cur":"USD"}}
  """.stripMargin

    val response = OpenRtbResponseLogEntry.fromJson(log)
    println(response)

    response.time should be (1453884999892l)
    response.tpid should be (38)
    response.requestId should be ("41831145388499984901T038")

    val seat = response.response.seatbid
    val bid =  seat.get.head.bid.head
    bid.price should be (0.19548)
    bid.crid should be ("16478941")
    bid.adomain.head should be ("ad4mat.com")

    bid.ext.get.avn should be ("DE - Reach")
  }
}