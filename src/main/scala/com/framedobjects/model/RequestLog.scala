package com.framedobjects.model

import java.net.URLDecoder

case class RequestLog(
  userID: String,
  impressionId: String,
  timeStamp: String,
  referrerUrl: String,
  userAgentName: String,
  userAgentVersion: String,
  userAgentOpertingSystem: String,
  sourceIp: String,
  iFrame: String,
  newUser: String,
  slotId: String,
  requestUri: String,
  queryString: String)

object RequestLog {

  def toRequestLog(logEntry: String): RequestLog = {
    val logEntries = logEntry.split(",")
    RequestLog(logEntries(0), logEntries(1), logEntries(2), logEntries(3), logEntries(4), logEntries(5), logEntries(6), logEntries(7), logEntries(8),
      logEntries(9), logEntries(10), logEntries(11), URLDecoder.decode(logEntries(12), "UTF-8"))
  }
}