package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class AppnexusResponseLogentrySpec extends FlatSpec with Matchers {

  "A bid" should "be parsed correctly" in {
    val log = """{"external_auction_id":"40143144105888167501T045","appnexus_auction_id":1312742309589651641,"request_error":false,"bid":0.271232,"buyer_member_id":1882,"creative_id":28391740,"landing_page_url":"http://www.drk.de","brand_id":218,"ad_tag":"<IFRAME SRC='http://ams1.ib.adnxs.com/ab?e=wqT_3QLcBPBCUwIAAAIA1gAFCMKgk68FELmh3qXOhfObEhiqh7T5sPzrizYgASotCYMpIQSiSdM_EcMLUZ3eW9E_GXWTGARWDtU_IRESBCmDDSSwMLTHsAI4_QpA2g5IAlC88sQNWN3nKWAAaJLUAnAAeKXwA4ABAYoBA1VTRJIBAQbwQJgB2AWgAVqoAQGwAQC4AQHAAQXIAQDQAQDYAQDgAQDwAQCKAld1ZignYScsIDQ1Mzg4MywgMTQ0MTA1ODg4Mik7ARwoYycsIDkwNzU0NTVGHQAscicsIDI4MzkxNzQwNh4A8IqSAq0BIVlpZWg3UWpfOWFrRUVMenl4QTBZQUNEZDV5a3dBRGdBUUFCSTJnNVF0TWV3QWxnQVlKb0hhQUJ3QkhnV2dBRVVpQUVXa0FFQm1BRUJvQUVCcUFFRHNBRUF1UUdES1NFRW9rblRQOEVCZ3lraEJLSkowel9KQVNueVUtSENqZXNfMlFFQUFBAQNkRHdQLUFCbE1nTDZnRUhNalE1TnpFeE1fVUIBHjBQdy4umgIdIVhnZnBSOrAA8JEzZWNwSUFBLrICEjUzNTI2MTQzMTcyODEyOTAyN9gC1QPgAtjGBuoCMWh0dHA6Ly93aXBvbmV3cy5kZS9yb3RhdGlvbi9hZHNjYWxlLzcyOC9pbmRleC5waHCAAwGIAwGQAwCYAxCgAwGqAwCwAwC4AwDAA6wCyAMA2AMA4AMA6AMA8AMA-AMDgAQAkgQEL2FzaQ..&s=975b8c43926819ebf726e7de9265bd553b59ecf5&dlo=1&referrer=http%3A%2F%2Fwiponews.de%2Frotation%2Fadscale%2F728%2Findex.php&pp=%%pricepaid%%' FRAMEBORDER='0' SCROLLING='no' MARGINHEIGHT='0' MARGINWIDTH='0' TOPMARGIN='0' LEFTMARGIN='0' ALLOWTRANSPARENCY='true' WIDTH='728' HEIGHT='90'><\/IFRAME>","no_bid":false}"""

    val bid = AppnexusResponseLogEntry.fromJson(log)
    bid.no_bid should be(false)
    bid.bid should be (Some(0.271232))
  }

  "A no bid" should "be parsed correctly" in {
    val log = """{"external_auction_id":"40109144105880601701T045","request_error":true,"no_bid":true,"request_error_id":5}"""

    val bid = AppnexusResponseLogEntry.fromJson(log)
    bid.no_bid should be(true)
    bid.bid should be (None)
  }
}