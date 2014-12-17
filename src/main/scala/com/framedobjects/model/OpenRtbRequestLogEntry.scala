package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbRequestLogEntry(
  time: Long,
  tpid: Int,
  request: Request)

case class Request(id: String)

object OpenRtbRequestLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbRequestLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbRequestLogEntry]
  }
}