package com.framedobjects.model

import org.scalatest.Matchers
import org.scalatest.FlatSpec

class AdscaleResponseLogEntrySpec extends FlatSpec with Matchers {
  
  "A log line" should "convert into an Adscale response object" in {
    val logEntry = "{\"bid\":{\"cpm\":0,\"tag\":null,\"cid\":null,\"rid\":null,\"lpu\":null,\"avn\":null,\"agn\":\"Criteo\"}}"
      
    val response = AdscaleResponseLogEntry.fromJson(logEntry)
    println(response)
    response.bid.cpm should be (0)
  } 
}