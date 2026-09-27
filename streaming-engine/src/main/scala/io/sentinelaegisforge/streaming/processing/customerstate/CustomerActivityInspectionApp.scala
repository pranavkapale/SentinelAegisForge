package io.sentinelaegisforge.streaming.processing.customerstate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{asc, desc}

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession

object CustomerActivityInspectionApp {
  def main(args: Array[String]): Unit = {
    val arguments = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val deltaPath = arguments.getOrElse(
      "delta-path",
      CustomerActivityConfig.DefaultSnapshotDeltaPath
    )
    val master = arguments.getOrElse(
      "spark-master",
      CustomerActivityConfig.DefaultSparkMaster
    )

    val spark = TransactionSparkSession.create(master)
    spark.sparkContext.setLogLevel("WARN")
    try {
      require(DeltaTable.isDeltaTable(spark, deltaPath), s"No Delta table exists at $deltaPath")
      val snapshots = spark.read.format("delta").load(deltaPath)
      println(s"Customer activity snapshot inspection: path=$deltaPath rows=${snapshots.count()}")
      snapshots
        .orderBy(asc("state_latest_event_time"), asc("customer_id"), desc("lifecycle_type"))
        .show(100, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def parseArguments(args: Array[String]): Either[String, Map[String, String]] =
    args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (parts(0).nonEmpty && parts(1).nonEmpty && SupportedArguments.contains(parts(0))) {
          Right(values.updated(parts(0), parts(1)))
        } else {
          Left(s"Invalid argument: $argument")
        }
      case (Right(_), argument) => Left(s"Expected --name=value argument, received: $argument")
      case (left @ Left(_), _)  => left
    }

  private val SupportedArguments = Set("delta-path", "spark-master")
}
