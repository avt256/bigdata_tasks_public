/**
  *  Run in spark shell (2.4.0). Use paste mode to process script.
  *  spark-shell --conf spark.driver.args=example.csv
  *  :paste task-sessions-dataframe.scala
  *
  *  Enriches events with sessions using dataframe api window functions and calculates other statistics for task 2.
  *
  */

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
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
import sqlContext.implicits._

val filepath = sqlContext.getConf("spark.driver.args")
val initDf = sqlContext.read.option("header","true").schema(customSchema).csv(filepath)

// calc and show sessions using lag window function

println("initial events")
initDf.show(100)

val SessionInterval = 300
val windowSpec = Window.partitionBy($"userId", $"category").orderBy($"eventTime")

// finds all sessions for events with interval > 5 min, also split session by product change is flag is set

def makeSessionsDf(isBreakByProduct: Boolean) =
  initDf
    .withColumn("prevEventTime", lag($"eventTime", 1) over windowSpec)
    .withColumn("nextEventTime", lag($"eventTime", -1) over windowSpec)
    .withColumn("nextProduct", lag($"product", -1) over windowSpec)
    .withColumn("prevDiff", $"eventTime".cast(LongType) - $"prevEventTime".cast(LongType))
    .withColumn("nextDiff", $"nextEventTime".cast(LongType) - $"eventTime".cast(LongType))
    .filter($"prevDiff" > SessionInterval || ($"nextProduct" =!= $"product" && isBreakByProduct) || $"prevDiff".isNull || $"nextDiff" > SessionInterval || $"nextDiff".isNull)
    .withColumn("sessionStartTime", lag($"eventTime", 1) over windowSpec)
    .filter($"nextDiff" > SessionInterval || $"nextDiff".isNull || ($"nextProduct" =!= $"product" && isBreakByProduct))
    .withColumn("sessionStartTime", when($"prevDiff" > SessionInterval, $"eventTime").otherwise($"sessionStartTime"))
    .withColumn("sessionStartTime", when($"sessionStartTime".isNull, $"eventTime").otherwise($"sessionStartTime"))
    .withColumn("sessionId", monotonically_increasing_id())
    .withColumnRenamed("eventTime", "sessionEndTime")
    .select("category", "product", "userId", "sessionStartTime", "sessionEndTime", "sessionId")

val sessionsDf = makeSessionsDf(isBreakByProduct = false)
println("found sessions")
sessionsDf.show(100)

// joins initial events with found sessions

println("initial event enriched with sessions")

val enrichedWithSessionsDf =
  initDf.join(sessionsDf,
    initDf("userId") === sessionsDf("userId") &&
    initDf("category") === sessionsDf("category") &&
    initDf("eventTime") <= sessionsDf("sessionEndTime") &&
    initDf("eventTime") >= sessionsDf("sessionStartTime"))

enrichedWithSessionsDf
  .select(initDf("*"), sessionsDf("sessionId"), sessionsDf("sessionStartTime"), sessionsDf("sessionEndTime"))
  .show(100)


// sessions with durations

val durationsDf =
  sessionsDf
    .withColumn("sessDuration", $"sessionEndTime".cast(LongType) - $"sessionStartTime".cast(LongType))
      .drop("sessionStartTime", "sessionEndTime")

println("median session duration per category")

val mediansDf =
  durationsDf
    .select($"category".as[String], $"sessDuration".as[Long])
    .groupByKey(_._1)
    .mapGroups { case (key, durations) =>
        val seq = durations.map(_._2).toArray
        val sorted = seq.sorted
        key -> sorted((seq.length + 1) / 2)
    }.toDF("category", "median session duration")


mediansDf.show(100)

println("calc num unique users per category and duration type")

val durationsTypes =
  durationsDf
  .withColumn("durationType",
    when($"sessDuration" > 300, "long")
      .when($"sessDuration" > 60, "middle")
      .otherwise("short"))
  .select($"category", $"durationType", $"userId").groupBy($"category", $"durationType").agg(countDistinct($"userId"))

durationsTypes.show(100)


val durationsByProductDf =
  makeSessionsDf(isBreakByProduct = true)
    .withColumn("sessDuration", $"sessionEndTime".cast(LongType) - $"sessionStartTime".cast(LongType))

println("sessions split by product change")
durationsByProductDf.show(100)

println("top ranked products by session duration")

val windowSpec2 = Window.partitionBy($"category").orderBy($"sessDuration".desc)
val durationsRanks =
  durationsByProductDf
    .drop("eventType", "sessionEndTime", "sessionStartTime", "sessionId")
    .withColumn("rank", rank() over windowSpec2)
    .filter($"rank" <= 10)
durationsRanks.show(100)


