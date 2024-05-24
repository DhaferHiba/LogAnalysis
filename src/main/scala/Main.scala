import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import vegas._
import vegas.sparkExt._
case class LogEntry(ip: String, clientId: String, userId: String, dateTime: String, method: String, endpoint: String, protocol: String, responseCode: Int, contentSize: Long)

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val logFile = "D:/untitled8/file.log"

    val spark = SparkSession.builder
      .appName("Log Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logDF = spark.read.textFile(logFile)
      .map(parseLogLine)
      .toDF()
      .cache()

    logDF.show(5, truncate = false)

    // Analyse des erreurs courantes
    val errorDF = logDF.filter($"responseCode" >= 400).groupBy("responseCode", "endpoint")
      .agg(count("*").alias("hits"))
      .orderBy(desc("hits"))
    errorDF.show(5, truncate = false)

    // Comportement des utilisateurs
    val userBehaviorDF = logDF.groupBy("ip", "endpoint")
      .agg(count("*").alias("hits"))
      .orderBy(desc("hits"))
    userBehaviorDF.show(5, truncate = false)

    // Métriques de performance
    val performanceMetricsDF = logDF.agg(
      avg("contentSize").alias("avgContentSize"),
      max("contentSize").alias("maxContentSize"),
      min("contentSize").alias("minContentSize")
    )
    performanceMetricsDF.show(5, truncate = false)

    // Visualisation avec Vegas
    val plot = Vegas("Hits per Endpoint").
      withDataFrame(userBehaviorDF).
      encodeX("endpoint", Nom).
      encodeY("hits", Quant).
      mark(Bar)

    plot.show
    // Visualisation avec Vegas
    val errorPlot = Vegas("Errors per Endpoint").
      withDataFrame(errorDF).
      encodeX("responseCode", Nom).
      encodeY("hits", Quant).
      mark(Bar)

    errorPlot.show
    spark.stop()
  }

  def parseLogLine(log: String): LogEntry = {
    val logPattern = """^(\S+) (\S+) (\S+) \[(\S+ \+\S+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-)$""".r
    log match {
      case logPattern(ip, clientId, userId, dateTime, method, endpoint, protocol, responseCode, contentSize) =>
        LogEntry(ip, clientId, userId, dateTime, method, endpoint, protocol, responseCode.toInt, if (contentSize == "-") 0 else contentSize.toLong)
      case _ => LogEntry("", "", "", "", "", "", "", 0, 0)
    }
  }
}
