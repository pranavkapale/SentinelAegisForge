package io.sentinelaegisforge.streaming.kafka.ingestion

import org.apache.spark.sql.SparkSession

object TransactionSparkSession {
  val ApplicationName: String = "SentinelAegisForge transaction ingestion"

  def builder(master: String): SparkSession.Builder =
    SparkSession
      .builder()
      .appName(ApplicationName)
      .master(master)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  def create(master: String): SparkSession = builder(master).getOrCreate()
}
