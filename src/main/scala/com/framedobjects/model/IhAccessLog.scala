package com.framedobjects.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.parse

case class IhAccessLog(
  time: Long,
  path: Option[String],
  ref: Option[String],
  ua: Option[String],
  query: String,
  ip: String,
  data: Option[Data]) {

  override def toString(): String = {
    s"$time\t${path.getOrElse("/pup")}\t$query\t${ref.getOrElse("no referrer header")}\t${ua.getOrElse("no user agent")}"
  }

  def hasWsParamValue(): Boolean = {
    val wsParam = query.split("&").filter(_.startsWith("ws"))
    if (wsParam.isEmpty)
      false
    else
      wsParam.head.split("=").size > 1
  }

  private def getPageReferrer(): String = {
    val queryParams = query.split("&")
    val referrer = queryParams.filter(_.startsWith("ref"))
    if (referrer.size == 1)
      referrer.head.split("=")(1)
    else
      "no referrer"
  }
}

case class Data(
  uid: String,
  iid: String,
  iidx: String,
  sid: Long,
  iFrame: Boolean,
  newUser: Boolean)

object IhAccessLog {

  implicit val formats = DefaultFormats

  def fromJson(jsonString: String): IhAccessLog = {
    val jValue = parse(jsonString)
    jValue.extract[IhAccessLog]
  }
}