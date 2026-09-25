package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.functions.desc

import io.delta.tables.DeltaTable
import io.sentinelaegisforge.streaming.kafka.ingestion.{
  TransactionIngestionConfig,
  TransactionSparkSession
}

object DeltaInspectionApp {
  def main(args: Array[String]): Unit = {
    val arguments = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val deltaPath = arguments.getOrElse("delta-path", DeltaIngestionConfig.DefaultDeltaPath)
    val master = arguments.getOrElse(
      "spark-master",
      TransactionIngestionConfig.DefaultSparkMaster
    )
    require(deltaPath.trim.nonEmpty, "delta-path must not be blank")

    val spark = TransactionSparkSession.create(master)
    spark.sparkContext.setLogLevel("WARN")

    try {
      require(
        DeltaTable.isDeltaTable(spark, deltaPath),
        s"No Delta table exists at $deltaPath"
      )
      val tableFrame = spark.read.format("delta").load(deltaPath)
      val rowCount = tableFrame.count()
      println(s"Delta table inspection: path=$deltaPath rows=$rowCount")
      tableFrame.printSchema()
      DeltaTable
        .forPath(spark, deltaPath)
        .history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .orderBy(desc("version"))
        .show(20, truncate = false)
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
