package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class AppnexusRequestLogEntry(
    an_user_id: String,
    ext_user_id: String,
    ext_auction_id: String,
    width: String,
    height: String,
    ad_format: String,
    ext_pub_code: String,
    ext_site_code: String,
    ext_placement_code: String,
    ip_address: String,
    user_agent: String)

object AppnexusRequestLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): AppnexusRequestLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[AppnexusRequestLogEntry]
  }
}