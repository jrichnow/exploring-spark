package com.framedobjects.model

import net.liftweb.json._


case class SspBidResponse(time: Long,
                          requestId: String,
                          tpid: Option[String],
                          response: Response)

case class Response(httpstatus: Option[String],
                    id: Option[String],
                    seatbid: Seq[SeatBid])

case class SeatBid(bid: Seq[ABid])

case class ABid(id: String,
                impid: String,
                price: Float,
                adid: String)

object SspBidResponse {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): SspBidResponse = {
    val jValue = parse(jsonString)
    jValue.extract[SspBidResponse]
  }
}
