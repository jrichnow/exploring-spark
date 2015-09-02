package com.framedobjects.model

case class AdscaleNotification(
    iid: String, 
    uid: String,
    tpuid: String,
    t: String,
    cid: Option[String],
    rid: Option[String],
    win: Boolean,
    price: Double,
    r: Option[String])

object AdscaleNotification {
  
  def fromLog(log: String): AdscaleNotification = {
    val queryParams = log.split("&").toSeq
    AdscaleNotification(
        getValue(queryParams, "iid").get,
        getValue(queryParams, "uid").get,
        getValue(queryParams, "tpuid").get,
        getValue(queryParams, "t").get,
        getValue(queryParams, "cid"),
        getValue(queryParams, "rid"),
        getValue(queryParams, "win").get.toBoolean,
        getValue(queryParams, "p").get.toDouble,
        getValue(queryParams, "r"))
  }
  
  private def getValue(queryParams: Seq[String], key: String): Option[String] = {
    val queryParam = queryParams.filter(_.startsWith(s"$key="))
    if (queryParam.length == 1) {
      Some(queryParam.head.split("=")(1))
    }
    else 
      None
  }
}