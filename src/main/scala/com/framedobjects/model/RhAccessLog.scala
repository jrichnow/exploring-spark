package com.framedobjects.model

case class RhAccessLog(
    time: String,
    pixelId: String,
    referrer: String,
    ua: String,
    impressionId: String)

object RhAccessLog {
  
  def fromLog(logEntry: String): RhAccessLog = {
    val logArray = logEntry.split("\\|\\|\\|")
    RhAccessLog(logArray(0), logArray(1), logArray(2), logArray(3), logArray(4))
  }
}