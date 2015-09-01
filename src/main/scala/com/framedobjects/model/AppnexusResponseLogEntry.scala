package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class AppnexusResponseLogEntry(
  no_bid: Boolean,
  bid: Option[Double])

object AppnexusResponseLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): AppnexusResponseLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[AppnexusResponseLogEntry]
  }
}