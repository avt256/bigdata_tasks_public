/**
  *  Run in spark shell (2.4.0). Only script load. Paste mode is not properly working for this script.
  *  spark-shell --conf spark.driver.args=example.csv
  *  :load task1-aggregator.scala
  *
  *  Enriches events with sessions using spark aggregator.
  *
  */


import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

val sqlContext = SparkSession.active.sqlContext
import sqlContext.implicits._
val filepath = sqlContext.getConf("spark.driver.args")
val df = sqlContext.read.option("header","true").csv(filepath)

case class Event(userId: String, category: String, product: String, eventType: String, eventTime: Timestamp)
case class Session(userId: String, category: String, product: String, eventType: String, sessionStart: Timestamp, sessionEnd: Timestamp)

// collects events for each key, during finish tries to find all the sessions within events and produces enriched events with sessions info

val sessionsAggregator = new Aggregator[Event, ArrayBuffer[Event], ArrayBuffer[(Event, String, Timestamp, Timestamp)]] with Serializable {
  def zero: ArrayBuffer[Event] = ArrayBuffer.empty
  def reduce(buffer: ArrayBuffer[Event], event: Event) = {
    buffer += event
  }
  def merge(buf1: ArrayBuffer[Event], buf2: ArrayBuffer[Event]) = { buf1 ++= buf2; buf1}

  def finish(events: ArrayBuffer[Event]): ArrayBuffer[(Event, String, Timestamp, Timestamp)] = {
    val sesBuf: ArrayBuffer[(Timestamp, Timestamp)] = ArrayBuffer.empty
    var prevTimeOpt: Option[Timestamp] = None
    var startTimeOpt: Option[Timestamp] = None

    events.sortBy(_.eventTime.getTime).map(_.eventTime).foreach { time =>
      (time, prevTimeOpt, startTimeOpt) match {
        case (newTime, Some(prevTime), Some(startTime)) if time.getTime - prevTime.getTime > 300000 =>
          sesBuf += startTime -> prevTime
          startTimeOpt = Some(newTime)
          prevTimeOpt = Some(newTime)

        case (newTime, Some(prTime), Some(stTime)) =>
          prevTimeOpt = Some(newTime)

        case (newTime, None, None) =>
          startTimeOpt = Some(newTime)
          prevTimeOpt = Some(newTime)
      }
    }
    if (startTimeOpt.isDefined) {
      sesBuf += startTimeOpt.get -> prevTimeOpt.get
    }

    events.map { event =>
      val session = sesBuf.find(session => session._1.getTime <= event.eventTime.getTime && session._2.getTime >= event.eventTime.getTime).get
      (event, event.userId + event.category + sesBuf.indexOf(session), session._1, session._2)
    }
  }
  def bufferEncoder: Encoder[ArrayBuffer[Event]] = Encoders.kryo[ArrayBuffer[Event]]
  def outputEncoder: Encoder[ArrayBuffer[(Event, String, Timestamp, Timestamp)]] = Encoders.kryo[ArrayBuffer[(Event, String, Timestamp, Timestamp)]]
}.toColumn


val ds = df.as[Event]
ds.show(100)

ds.groupByKey(event => event.userId -> event.category)
.agg(sessionsAggregator)
.flatMap { case (key, events) => events.map(ev => (ev._1.category, ev._1.userId, ev._1.product, ev._1.eventTime, ev._2, ev._3, ev._4 )) }
.toDF("category", "userId", "product", "eventTime", "sessionId", "sessionStartTime", "sessionEndTime")
.show(100)
