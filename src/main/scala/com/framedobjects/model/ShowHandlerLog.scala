package com.framedobjects.model

case class ShowHandlerLog (iid: String, slotId: String, advertId: String, cost: String)

object  ShowHandlerLog {

  def fromLog(log: String): ShowHandlerLog = {
    val logEntries = log.split(", ")

    val slotId = logEntries(5).split("=")(1)
    val advertId = logEntries(6).split("=")(1)
    val iid = logEntries(8).replace("'", "").split("=")(1)
    val query = logEntries(11)
    val cost = query.split("&")(6).split("=")(1)

    ShowHandlerLog(iid, slotId, advertId, cost)
  }
}
