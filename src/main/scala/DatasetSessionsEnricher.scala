import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

case class Event(userId: String, category: String, product: String, eventType: String, eventTime: Timestamp)

case class EventWithSession(event: Event, sessionStartTime: Timestamp, sessionId: String)

/**
  * Enriches events with sessions using datasets functions (no end session column)
  */
class DatasetSessionsEnricher(sparkSession: SparkSession, filepath: String) {

  def run(): Unit = {
    import sparkSession.sqlContext
    import sqlContext.implicits._

    val df = sqlContext.read.option("header", "true").csv(filepath)
    val ds = df.as[Event]

    val eventsWithSessionsDs: Dataset[EventWithSession] =
      ds
        .sort($"eventTime")
        .groupByKey(event => event.userId -> event.category)
        .flatMapGroups { case (key, eventsIt) =>
          var prevTimeOpt: Option[Timestamp] = None
          var startTimeOpt: Option[Timestamp] = None
          val sesBuf: ArrayBuffer[(Timestamp, String)] = ArrayBuffer.empty

          eventsIt.map { event =>
            def buildId(eventId: Long) = Seq(key._1, key._2, eventId).mkString("-")

            def eventWithCurrentSession = {
              EventWithSession(event, startTimeOpt.get, buildId(sesBuf.size))
            }

            (event.eventTime, prevTimeOpt, startTimeOpt) match {
              case (newTime, Some(prevTime), Some(startTime)) if event.eventTime.getTime - prevTime.getTime > 300000 =>
                sesBuf += Tuple2(startTime, buildId(sesBuf.size))
                startTimeOpt = Some(newTime)
                prevTimeOpt = Some(newTime)
                eventWithCurrentSession

              case (newTime, Some(prTime), Some(stTime)) =>
                prevTimeOpt = Some(newTime)
                eventWithCurrentSession

              case (newTime, None, None) =>
                startTimeOpt = Some(newTime)
                prevTimeOpt = Some(newTime)
                EventWithSession(event, newTime, buildId(sesBuf.size))
            }
          }
        }

    eventsWithSessionsDs.show(100, truncate = false)
  }
}
