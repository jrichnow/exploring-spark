package com.framedobjects.model

case class CounterRecord (
  slotId: String,
  advertId: String,
  time: String,
  ip: String,
  userId: String,
  cost: String,
  typeId: Int,
  newUser: String,
  impressionId: String)

object CounterRecord {

  def fromRecord(record: String): CounterRecord = {
    val r = record.split(",")

    CounterRecord(r(0), r(1), r(2), r(3), r(4), r(5), r(6).toInt, r(7), r(8))
  }
}