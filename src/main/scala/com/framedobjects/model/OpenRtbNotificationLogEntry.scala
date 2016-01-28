package com.framedobjects.model

import net.liftweb.json._

case class OpenRtbNotificationLogEntry(time: Long, tpid: Int, notifications: NotificationWrapper)
case class NotificationWrapper(notifications: Seq[SingleNotification])
case class SingleNotification(id: String, impid: String, dealid: Option[String], win: Boolean, p: Double, r: Option[String])

object OpenRtbNotificationLogEntry {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): OpenRtbNotificationLogEntry = {
    val jValue = parse(jsonString)
    jValue.extract[OpenRtbNotificationLogEntry]
  }
}