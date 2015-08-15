package com.framedobjects

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.apache.devicemap.DeviceMapFactory
import org.apache.devicemap.loader.LoaderOption
import ua.parser.Parser

class UserAgentParsingSpec extends FlatSpec with Matchers  {

  "user agent" should "be parsed" in {
    val ua = "Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko"
    val uap = DeviceMapFactory.getClient(LoaderOption.JAR)
    val device = uap.classifyDevice(ua)
    
    println(device)
  }
  
  "uap-scala" should "parse a user agent" in {
	val ua = "Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko"
	  val x = this.getClass().getResourceAsStream("regexes.yaml")
	  Parser.create(x)
	  
//    val client = Parser.get.parse(ua)
//    
//    println(client)
  } 
}