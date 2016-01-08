package com.framedobjects.model

import org.scalatest.{Matchers, FlatSpec}

import scala.Some


class SspBidResponseSpec extends FlatSpec with Matchers {

  "A zero bid response with tpid field" should "convert to an object" in {
    val jsonString =
      """
        {"time":1452123755653,"requestId":"1693e06f-fac0-46db-b92a-82a6285fc214","tpid":"101","response":{"http-status":204}}
      """
    val sspBidResponse = SspBidResponse.fromJson(jsonString)

    sspBidResponse.time should equal (1452123755653l)
    sspBidResponse.requestId should be ("1693e06f-fac0-46db-b92a-82a6285fc214")
    sspBidResponse.tpid.get should be ("101")
//    sspBidResponse.response.httpstatus should be (204)
  }

  "A full bid response" should "convert to an object" in {
    val jsonString =
      """
        {"time":1452123755660,"requestId":"03d08137-af6f-4043-91d8-b053ea1c489b","tpid":"102","response":{"id":"03d08137-af6f-4043-91d8-b053ea1c489b","seatbid":[{"bid":[{"id":"301501452123755606","impid":"0","price":0.1107991,"adid":"94401","adm":"<iframe width='160' height='600' frameborder='0' marginwidth='0' marginheight='0' scrolling='no' allowTransparency='true' src='http://dh.adscale.de/show?aid=NWMzMDQw&sid=NjA2MTQw&ck=YmM5MTgw&iid=301501452123755606&iidx=00&hid=301&cst=${AUCTION_PRICE}&uu=424771427565651427&nu=0&tpid=101'><\/iframe>","adomain":["base.de"],"crid":"\"null\"","ext":{"width":160,"height":600,"clickThroughUrl":"base.de"}}]}],"cur":"EUR"}}
      """
    val sspBidResponse = SspBidResponse.fromJson(jsonString)

    sspBidResponse.time should equal (1452123755660l)
    sspBidResponse.requestId should be ("03d08137-af6f-4043-91d8-b053ea1c489b")
    sspBidResponse.tpid.get should be ("102")
    sspBidResponse.response.id.get should be ("03d08137-af6f-4043-91d8-b053ea1c489b")

    val bid = sspBidResponse.response.seatbid.head.bid.head
    bid.id should be ("301501452123755606")
    bid.impid should be ("0")
    bid.price should be (0.1107991f)
  }
}
