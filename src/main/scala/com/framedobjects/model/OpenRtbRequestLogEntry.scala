package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class OpenRtbRequestLogEntry(time: Long, tpid: Int, request: Request)
case class Request(id: String, imp: Seq[Impression], site: Site, user: User) {
  def impressionId: String = {
    id.substring(0,18)
  }
}

case class Impression(id: String, tagid: String, pmp: Option[Pmp])
case class Site(id: String, domain: Option[String], ref: Option[String], page: Option[String], publisher: Publisher)
case class Publisher(id: String)

case class Pmp(deals: Seq[Deal])
case class Deal(id: String, bidfloor: Double)

case class User(id: String)

object OpenRtbRequestLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbRequestLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbRequestLogEntry]
  }
}