package com.framedobjects.model

import org.scalatest.{Matchers, FlatSpec}

class ShowHandlerLogSpec  extends FlatSpec with Matchers {

  "A log file entry" should "be parsed" in {
    val log =
    """
      2016-01-06 23:47:54,995 DEBUG http-apr-8112-exec-241 [org.adscale.adserver.show.ShowHandler] received valid request CoreProcessedValues{freqCapHour=null, freqCapDay=null, freqCapCallbackDenial=null, freqCapIntelligentTrafficBuy=5#593200247#0006#0#1, isValidRequest=true, slotId=113751, advertId=78351, isValidChecksum=true, impressionId='301641452124073631', requestTime=Wed Jan 06 23:47:54 UTC 2016, sourceIp='185.25.121.250', queryString='aid=NGM4M2Mw&sid=NmYxNWMw&ck=YmI5OTgw&iid=301641452124073631&iidx=03&hid=301&cst=0.059000&uu=422041436171787979&nu=0&tpid=101', lastError='null', tempCpxCookie=null, callbackTrackingValues=1#0, hid='301', sslRequest=false, badCookies=[], timeSinceImpression=1364, advertTrackingValues=null, nuggadValue=NuggadValue [version=2, creationTimeInMinutes=0, creationTime=Thu Jan 01 00:00:00 UTC 1970, websiteIdList=[]], targeting=null, nuggadTargeting=[], thirdPartyUserIdValues=null, userOptOut=false, userTracking=UserTracking{advertTrackingValue=AdvertTrackingValue{postViewValues={}, clickTrackValues={}}, callbackDenialValue=FreqCapValue{freqCapInfos={}, refDate=Wed Jan 06 00:00:00 UTC 2016, timeUnit=HOUR}, freqCapDayValue=FreqCapValue{freqCapInfos={}, refDate=Wed Jan 06 00:00:00 UTC 2016, timeUnit=DAY}, freqCapHourValue=FreqCapValue{freqCapInfos={}, refDate=Wed Jan 06 00:00:00 UTC 2016, timeUnit=HOUR}, thirdPartyUserIdValue=ThirdPartyUserIdValue [infos={}, dirty=false, lastDhmUpdate=Thu Jan 01 00:00:00 UTC 1970, refDate=Wed Jan 06 00:00:00 UTC 2016], userProfileValue=null, userId='422041436171787979', userRetargeted=false, newUser=false, optedOutValue=OptOutValue [isOptOut=falsedirty=false, lastDhmUpdate=Thu Jan 01 00:00:00 UTC 1970], trackingResistantUser=false, userIdGeneratedThisRequest=false}, advertTrackingCookieOnrequest=false, uniqueUserCookie=org.adscale.adserver.cookies.cookie.UniqueUserCookie@21697d7d[value=422041436171787979,newUser=false,domain=.adscale.de,domainPath=/,maxAgeInSec=31336000], thirdPartyUserIdOnRequest=false, uniqueUserLogger=null, freqCapCookiesOnRequest=false, callbackDenialOnRequest=false, publisherClickTrackingUrl=null}
    """
    val logEntries = log.split(", ")

    val showHandlerLog = ShowHandlerLog.fromLog(log)

    showHandlerLog.iid should be ("301641452124073631")
    showHandlerLog.advertId should be ("78351")
    showHandlerLog.slotId should be ("113751")
    showHandlerLog.cost should be ("0.059000")
  }
}
