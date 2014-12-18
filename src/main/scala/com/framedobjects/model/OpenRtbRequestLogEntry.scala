package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbRequestLogEntry(
  time: Long,
  tpid: Int,
  request: Request)

case class Request(id: String, imp: Seq[Impression], site: Site, user: User)

case class Impression(id: String, tagid: String)
case class Site(id: String, publisher: Publisher)
case class Publisher(id: String)

case class User(id: String)

object OpenRtbRequestLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbRequestLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbRequestLogEntry]
  }
}