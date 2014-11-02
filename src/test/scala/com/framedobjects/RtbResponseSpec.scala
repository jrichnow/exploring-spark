package com.framedobjects

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class RtbResponseSpec extends FlatSpec with Matchers {

  "A JSON string" should "convert to an object" in {
    val jsonString = """
      { "timestamp": "2014-10-29T21:06:14.174-05:00Z", "impression_id": 436211414616774136, "time_remaining": 33, "response": {"bids":[{"aid":134834,"ecpm":0.40},{"aid":137520,"ecpm":0.380}],"iid":"436211414616774136"} }
      """
    val rtbResponse = RtbResponse.fromJson(jsonString)
    
    rtbResponse.impression_id should equal (436211414616774136L)
  }
  
  "can" should "split" in {
    val line = """ 
      2014-10-29 21:06:08,101|{ "timestamp": "2014-10-29T21:06:08.101Z", "impression_id": 436231414616768073, "time_remaining": 24, "response": {"bids":[{"aid":137519,"ecpm":0.390}],"iid":"436231414616768073"} }
      """
    val splitLine = line.split("\\|")
    println("splitting: " + splitLine(1))
  }
}