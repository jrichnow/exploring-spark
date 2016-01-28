package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbResponseLogEntry(
                                    time: Long,
                                    tpid: Int,
                                    requestId: String,
                                    responseTime: Int,
                                    response: BidResponse)

case class BidResponse(id: Option[String], seatbid: Option[Seq[Seats]])

case class Seats(bid: Seq[Bid])

case class Bid(id: String, impid: String, price: Double, crid: String, adomain: Seq[String], ext: Option[BidExtension])

case class BidExtension(avn: String, agn: String)

object OpenRtbResponseLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbResponseLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbResponseLogEntry]
  }
}