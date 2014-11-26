package com.framedobjects

import net.liftweb.json._
import org.joda.time.DateTime
import net.liftweb.json.JObject

case class RtbResponse(
  timestamp: String,
  impression_id: Long,
  time_remaining: Int,
  response: JObject)

object RtbResponse {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): RtbResponse = {
    val jValue = parse(jsonString)
    jValue.extract[RtbResponse]
  }
}