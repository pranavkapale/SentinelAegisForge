package io.sentinelaegisforge.streaming.processing.rollingfeatures

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.asc

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession

object TransactionRollingFeatureInspectionApp {
  def main(args: Array[String]): Unit = {
    require(args.forall(_.startsWith("--")), "Expected --name=value arguments")
    val options = args
      .map(_.drop(2).split("=", 2))
      .map { parts =>
        require(parts.length == 2 && Set("delta-path", "spark-master").contains(parts(0)))
        parts(0) -> parts(1)
      }
      .toMap
    val path = options.getOrElse("delta-path", RollingFeatureConfig.DefaultFeatureDeltaPath)
    val spark = TransactionSparkSession.create(
      options.getOrElse("spark-master", RollingFeatureConfig.DefaultSparkMaster)
    )
    spark.sparkContext.setLogLevel("WARN")
    try {
      require(DeltaTable.isDeltaTable(spark, path), s"No feature Delta table exists at $path")
      val features = spark.read.format("delta").load(path)
      println(s"Transaction customer feature inspection: path=$path rows=${features.count()}")
      features
        .select(
          "event_id",
          "customer_id",
          "event_time",
          "amount",
          "currency",
          "prior_transaction_count_5m",
          "prior_amount_sum_10m"
        )
        .orderBy(asc("event_time"), asc("event_id"))
        .show(100, truncate = false)
    } finally {
      spark.stop()
    }
  }
}
