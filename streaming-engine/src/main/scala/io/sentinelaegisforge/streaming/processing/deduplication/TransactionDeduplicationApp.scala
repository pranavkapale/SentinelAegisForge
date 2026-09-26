package io.sentinelaegisforge.streaming.processing.deduplication

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.DeduplicatedTransactionDeltaSink

object TransactionDeduplicationApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession.create(config.sparkMaster)
    spark.sparkContext.setLogLevel("WARN")

    try {
      require(
        DeltaTable.isDeltaTable(spark, config.validatedDeltaPath),
        s"No validated Delta table exists at ${config.validatedDeltaPath}"
      )

      val source = spark.readStream
        .format("delta")
        .load(config.validatedDeltaPath)
      val deduplicated = TransactionDeduplicationTransformer.deduplicate(
        source,
        config.watermarkDelay
      )
      val sink = new DeduplicatedTransactionDeltaSink(
        config.deduplicatedDeltaPath,
        config.deltaTxnAppId
      )
      val query = deduplicated.writeStream
        .queryName("transaction-event-time-deduplication")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: DataFrame, batchId: Long) => sink.writeBatch(batch, batchId))
        .start()

      query.awaitTermination()
      val progress = query.recentProgress.toVector.map(DeduplicationProgress.from)
      DeduplicationProgressReporter.report(progress)
      println(
        s"Transaction deduplication completed: source=${config.validatedDeltaPath} target=${config.deduplicatedDeltaPath} txnAppId=${config.deltaTxnAppId} watermarkDelay=${config.watermarkDelay}"
      )
    } finally {
      spark.stop()
    }
  }

  private[deduplication] def parseArguments(
      args: Array[String]
  ): Either[String, TransactionDeduplicationConfig] = {
    val parsed = args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (parts(0).nonEmpty && parts(1).nonEmpty) Right(values.updated(parts(0), parts(1)))
        else Left(s"Invalid argument: $argument")
      case (Right(_), argument) => Left(s"Expected --name=value argument, received: $argument")
      case (left @ Left(_), _)  => left
    }

    parsed.flatMap { values =>
      Either.cond(
        values.keySet.subsetOf(SupportedArguments),
        TransactionDeduplicationConfig(
          validatedDeltaPath = values.getOrElse(
            "validated-delta-path",
            TransactionDeduplicationConfig.DefaultValidatedDeltaPath
          ),
          deduplicatedDeltaPath = values.getOrElse(
            "deduplicated-delta-path",
            TransactionDeduplicationConfig.DefaultDeduplicatedDeltaPath
          ),
          checkpointLocation = values.getOrElse(
            "checkpoint-location",
            TransactionDeduplicationConfig.DefaultCheckpointLocation
          ),
          deltaTxnAppId = values.getOrElse(
            "delta-txn-app-id",
            TransactionDeduplicationConfig.DefaultTxnAppId
          ),
          watermarkDelay = values.getOrElse(
            "watermark-delay",
            sys.env.getOrElse(
              "WATERMARK_DELAY",
              TransactionDeduplicationConfig.DefaultWatermarkDelay
            )
          ),
          sparkMaster = values.getOrElse(
            "spark-master",
            TransactionDeduplicationConfig.DefaultSparkMaster
          )
        ),
        s"Unsupported arguments: ${(values.keySet -- SupportedArguments).toVector.sorted.mkString(", ")}"
      )
    }
  }

  private val SupportedArguments = Set(
    "validated-delta-path",
    "deduplicated-delta-path",
    "checkpoint-location",
    "delta-txn-app-id",
    "watermark-delay",
    "spark-master"
  )
}
