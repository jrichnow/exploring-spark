package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbResponseLogEntry(
                                    time: Long,
                                    tpid: Int,
                                    requestId: String,
                                    responseTime: Int,
                                    response: BidResponse)

case class BidResponse(id: String, seatbid: Seq[BidWrapper])

case class BidWrapper(bid: Seq[Bid])

case class Bid(id: String, impid: String, price: Double)

object OpenRtbResponseLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbResponseLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbResponseLogEntry]
  }
}