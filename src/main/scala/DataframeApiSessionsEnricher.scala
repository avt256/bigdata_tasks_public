import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

/**
  * Enriches events with sessions using window functions and collects stat
  */
class DataframeApiSessionsEnricher(sparkSession: SparkSession, filepath: String) {

  def run(): Unit = {
    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.types._

    // read from csv file

    val sqlContext = SparkSession.active.sqlContext

    val customSchema = StructType(Array(
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("userId", StringType, true),
      StructField("eventTime", TimestampType, true),
      StructField("eventType", StringType, true)
    )
    )

    import sparkSession.sqlContext.implicits._

    val initDf = sqlContext.read.option("header", "true").schema(customSchema).csv(filepath)

    val eventWithSessions = makeEventsEnrichedWithSessionsDf(initDf, isBreakByProduct = false)
    println("events with sessions")
    eventWithSessions.show(100)

    def makeDistinctSessions(events: DataFrame, isIncludeProduct: Boolean) = {
      val df = events.select("category", "userId", "product", "sessionId", "sessionStartTime", "sessionEndTime")
      (if (!isIncludeProduct) df.drop("product") else df)
        .distinct()
    }

    val sessionsDf = makeDistinctSessions(eventWithSessions, isIncludeProduct = false)
    sessionsDf.cache()

    println("found sessions")
    sessionsDf.show(100)

    // sessions with durations

    val durationsDf =
      sessionsDf
        .withColumn("sessDuration", $"sessionEndTime".cast(LongType) - $"sessionStartTime".cast(LongType))
        .drop("sessionStartTime", "sessionEndTime")

//    println("sessions durations")
//    durationsDf.show(100)

    println("median session duration per category")

    val medians = {
      durationsDf.createOrReplaceTempView("durations")
      sqlContext
        .sql("SELECT category, percentile(sessDuration, 0.5) FROM durations GROUP BY category")
    }

    medians.show(100)

    println("calc num unique users per category and duration type")

    val durationsTypes =
      durationsDf
        .withColumn("durationType",
          when($"sessDuration" > 300, "long")
            .when($"sessDuration" > 60, "middle")
            .otherwise("short"))
        .select($"category", $"durationType", $"userId")
        .groupBy($"category", $"durationType")
        .agg(countDistinct($"userId"))

    durationsTypes.show(100)

    val durationsByProductDf =
      makeDistinctSessions(makeEventsEnrichedWithSessionsDf(initDf, isBreakByProduct = true), isIncludeProduct = true)
        .withColumn("sessDuration", $"sessionEndTime".cast(LongType) - $"sessionStartTime".cast(LongType))

    println("sessions split by product change")
    durationsByProductDf.show(100)

    println("top ranked products by session duration")

    val windowSpec2 = Window.partitionBy($"category").orderBy($"sessDuration".desc)
    val durationsRanks =
      durationsByProductDf
        .drop("eventType", "sessionEndTime", "sessionStartTime", "sessionId")
        .withColumn("row_num", row_number() over windowSpec2)
        .filter($"row_num" <= 10)
    durationsRanks.show(100)

    sessionsDf.unpersist()

  }

  // calc and show sessions using lag window function
  // finds all sessions for events with interval > 5 min, also split session by product change is flag is set

  private def makeEventsEnrichedWithSessionsDf(initDf: Dataset[Row], isBreakByProduct: Boolean) = {
    import sparkSession.sqlContext.implicits._

    val SessionInterval = 300
    val sessionGroupColumns = Seq($"userId", $"category")
    val windowSpecAround = Window.partitionBy(sessionGroupColumns: _*).orderBy($"eventTime")
    val windowSpecBefore = Window.partitionBy(sessionGroupColumns: _*).orderBy($"eventTime").rowsBetween(Long.MinValue, 0)
    val windowSpecAfter = Window.partitionBy(sessionGroupColumns: _*).orderBy($"eventTime").rowsBetween(0, Long.MaxValue)

    initDf
      .withColumn("prevEventTime", lag($"eventTime", 1) over windowSpecAround)
      .withColumn("nextEventTime", lead($"eventTime", 1) over windowSpecAround)
      .withColumn("nextProduct", lead($"product", 1) over windowSpecAround)
      .withColumn("prevProduct", lag($"product", 1) over windowSpecAround)
      .withColumn("prevDiff", $"eventTime".cast(LongType) - $"prevEventTime".cast(LongType))
      .withColumn("nextDiff", $"nextEventTime".cast(LongType) - $"eventTime".cast(LongType))
      .withColumn("sessionId", monotonically_increasing_id())
      .withColumn("sessionStartTime",
        when($"prevDiff" > SessionInterval || $"prevDiff".isNull || ($"prevProduct" =!= $"product" && isBreakByProduct), $"eventTime")
          .otherwise(null))
      .withColumn("sessionEndTime",
        when($"nextDiff" > SessionInterval || $"nextDiff".isNull || ($"nextProduct" =!= $"product" && isBreakByProduct), $"eventTime")
          .otherwise(null))
      .withColumn("sessionId", when($"sessionStartTime".isNull, null).otherwise($"sessionId"))
      .withColumn("sessionStartTime", max($"sessionStartTime") over windowSpecBefore)
      .withColumn("sessionEndTime", min($"sessionEndTime") over windowSpecAfter)
      .withColumn("sessionId", max("sessionId") over windowSpecBefore)
      .select("category", "product", "userId", "eventTime", "sessionId", "sessionStartTime", "sessionEndTime")
  }

}
