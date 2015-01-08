package com.framedobjects.model

case class Notification(
    impressionId: String,
    userId: String,
    winningPrice: Double,
    win: Boolean,
    reason: String,
    winningAdvertId: Long,
    tpuid: String) 

object Notification {

  def fromLogEntry(logEntry: String): Notification = {
    val notificationData = logEntry.split("\\[")(1).split("\\]")(0)
    val fieldArray = notificationData.split(", ")
    
    Notification(getValueForField("impressionId", fieldArray),
    			 getValueForField("userId", fieldArray),
    			 getValueForField("winningPrice", fieldArray).toDouble,
    			 getValueForField("win", fieldArray).toBoolean,
    			 getValueForField("reason", fieldArray),
    			 getValueForField("winningAdvertId", fieldArray).toLong,
    			 getValueForField("tpuid", fieldArray))
  }
  
  private def getValueForField(fieldName: String, fieldArray: Array[String]): String = {
    val valueArray = fieldArray.filter(_.contains(s"$fieldName="))(0).split("=")
    if (valueArray.length == 1) "" else valueArray(1)
  }
}