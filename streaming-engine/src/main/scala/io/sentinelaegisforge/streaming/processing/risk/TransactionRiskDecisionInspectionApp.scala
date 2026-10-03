package io.sentinelaegisforge.streaming.processing.risk

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession

object TransactionRiskDecisionInspectionApp {
  def main(args: Array[String]): Unit = {
    require(
      args.forall(a => a.startsWith("--delta-path=") || a.startsWith("--spark-master=")),
      "Expected --delta-path=... or --spark-master=..."
    )
    val path = args
      .find(_.startsWith("--delta-path="))
      .map(_.drop(13))
      .getOrElse(RiskDecisionConfig.DefaultTargetDeltaPath)
    val master = args.find(_.startsWith("--spark-master=")).map(_.drop(15)).getOrElse("local[*]")
    val spark =
      TransactionSparkSession.builder(master).appName("risk-decision-inspection").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val rows = spark.read.format("delta").load(path)
      println(s"Risk decision rows: ${rows.count()}")
      rows.printSchema()
      rows.orderBy("event_time", "kafka_partition", "kafka_offset").show(100, truncate = false)
    } finally spark.stop()
  }
}
