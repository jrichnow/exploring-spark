package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbResponseLogEntrySimple(
                                    time: Long,
                                    tpid: Int,
                                    requestId: String,
                                    responseTime: Int)

object OpenRtbResponseLogEntrySimple {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbResponseLogEntrySimple = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbResponseLogEntrySimple]
  }
}