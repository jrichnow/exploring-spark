package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class AdscaleResponseLogEntry(
  bid: AdscaleBid)

case class AdscaleBid(cpm: Double)

object AdscaleResponseLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): AdscaleResponseLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[AdscaleResponseLogEntry]
  }
}