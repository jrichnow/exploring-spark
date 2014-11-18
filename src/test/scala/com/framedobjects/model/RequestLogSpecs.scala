package com.framedobjects.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class RequestLogSpecs extends FlatSpec with Matchers {

  "A log line" should "convert to an object" in {
    val log = "983901396887607214,43681416249263023,0,http%3A%2F%2Fwww.ferien.de%2Fview%2Fbanner%2Fbanner-tag-iframe-extern%3FframeId%3DbannerIframe%26BxBannerNet%3DP4444.si.ferien.de_de%26BxBannerZone%3DHotels%26BxBannerTrackerString%3Dabflg%3D%3Bdest%3DBeliebtesteReiseziele%3Bregion%3D%3Banreise%3D20141120%3Babreise%3D20150216%3Bdauer%3D7%3Bpreis%3D%3Bpersonen%3D2%3Bkinder%3D0%3Bhkat%3D3%3Bztyp%3D%3Bverpfl%3DUEF%3Bmarke%3D%3Bstep%3D2%3B%26BxBannerOrd%3D503414419945329.44%26host%3Dhotel.ferien.de%26step%3D2,Mobile Safari,6.0,iOS 6,217.245.210.27,1,0,85020,/adscale-ih/impr,v=2&sid=NTMwNzAw&nu=0&t=1416249265419&iFrame&ssl=0&x=983901396887607214&pt=ads30%3D%26ads31%3DBeliebtesteReiseziele%26ads32%3D"
    val request = RequestLog.toRequestLog(log)
    
    request.userID should be("983901396887607214")
    request.impressionId should be("43681416249263023")
    request.timeStamp should be("0")
    request.referrerUrl should be("http%3A%2F%2Fwww.ferien.de%2Fview%2Fbanner%2Fbanner-tag-iframe-extern%3FframeId%3DbannerIframe%26BxBannerNet%3DP4444.si.ferien.de_de%26BxBannerZone%3DHotels%26BxBannerTrackerString%3Dabflg%3D%3Bdest%3DBeliebtesteReiseziele%3Bregion%3D%3Banreise%3D20141120%3Babreise%3D20150216%3Bdauer%3D7%3Bpreis%3D%3Bpersonen%3D2%3Bkinder%3D0%3Bhkat%3D3%3Bztyp%3D%3Bverpfl%3DUEF%3Bmarke%3D%3Bstep%3D2%3B%26BxBannerOrd%3D503414419945329.44%26host%3Dhotel.ferien.de%26step%3D2")
    request.userAgentName should be("Mobile Safari")
    request.userAgentVersion should be("6.0")
    request.userAgentOpertingSystem should be("iOS 6")
    request.sourceIp should be ("217.245.210.27")
    request.iFrame should be ("1")
    request.newUser should be ("0")
    request.slotId should be ("85020")
    request.requestUri should be ("/adscale-ih/impr")
    request.queryString should be ("v=2&sid=NTMwNzAw&nu=0&t=1416249265419&iFrame&ssl=0&x=983901396887607214&pt=ads30=&ads31=BeliebtesteReiseziele&ads32=")
  } 
}