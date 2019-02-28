import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * ./spark-submit --class SparkMain sessions-task_2.11-0.1.jar example.csv
  */
object SparkMain extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Sessions Stat")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val filepath = args(0)

    new DataframeApiSessionsEnricher(SparkSession.active, filepath)
      .run()
    new DatasetSessionsEnricher(SparkSession.active, filepath)
      .run()
}
