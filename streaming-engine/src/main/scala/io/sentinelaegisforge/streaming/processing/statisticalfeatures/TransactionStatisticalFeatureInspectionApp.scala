package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.asc

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession

object TransactionStatisticalFeatureInspectionApp {
  def main(args: Array[String]): Unit = {
    require(args.forall(_.startsWith("--")), "Expected --name=value arguments")
    val options = args
      .map(_.drop(2).split("=", 2))
      .map { parts =>
        require(parts.length == 2 && Set("delta-path", "spark-master").contains(parts(0)))
        parts(0) -> parts(1)
      }
      .toMap
    val path = options.getOrElse("delta-path", StatisticalFeatureConfig.DefaultTargetDeltaPath)
    val spark = TransactionSparkSession.create(
      options.getOrElse("spark-master", StatisticalFeatureConfig.DefaultSparkMaster)
    )
    spark.sparkContext.setLogLevel("WARN")
    try {
      require(DeltaTable.isDeltaTable(spark, path), s"No statistical Delta table exists at $path")
      val features = spark.read.format("delta").load(path)
      println(s"Transaction statistical feature inspection: path=$path rows=${features.count()}")
      features
        .select(
          "event_id",
          "customer_id",
          "event_time",
          "kafka_partition",
          "kafka_offset",
          "amount",
          "currency",
          "prior_transaction_count_5m",
          "prior_amount_sum_10m",
          "prior_amount_observation_count",
          "prior_amount_mean",
          "prior_amount_stddev",
          "amount_zscore",
          "statistical_feature_status"
        )
        .orderBy(asc("kafka_partition"), asc("kafka_offset"))
        .show(100, truncate = false)
    } finally spark.stop()
  }
}
