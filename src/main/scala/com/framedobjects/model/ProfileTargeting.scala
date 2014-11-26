package com.framedobjects.model

import net.liftweb.json._
import net.liftweb.json.JObject

case class ProfileTargeting(
    uu: String,
    publisherId: String,
    slotId: String,
    key: String,
    value: String)

object ProfileTargeting {
  
  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): ProfileTargeting = {
    val jValue = parse(jsonString)
    jValue.extract[ProfileTargeting]
  }
}