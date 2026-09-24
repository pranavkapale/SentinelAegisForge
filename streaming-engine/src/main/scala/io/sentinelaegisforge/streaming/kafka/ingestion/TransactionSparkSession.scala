package io.sentinelaegisforge.streaming.kafka.ingestion

import org.apache.spark.sql.SparkSession

object TransactionSparkSession {
  val ApplicationName: String = "SentinelAegisForge transaction ingestion"

  def create(master: String): SparkSession =
    SparkSession
      .builder()
      .appName(ApplicationName)
      .master(master)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
}
