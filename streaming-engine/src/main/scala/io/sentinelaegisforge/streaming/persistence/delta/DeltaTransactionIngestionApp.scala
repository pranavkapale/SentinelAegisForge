package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.{
  TransactionIngestionConfig,
  TransactionIngestionTransformer,
  TransactionKafkaSource,
  TransactionSparkSession,
  ValidatedTransactionRecord
}

object DeltaTransactionIngestionApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession.create(config.source.sparkMaster)
    spark.sparkContext.setLogLevel("WARN")

    val hook = config.failAfterDeltaBatchId
      .map(DeltaPostCommitHook.FailAfterBatch.apply)
      .getOrElse(DeltaPostCommitHook.NoOp)
    val sink = new DeltaTransactionSink(config.deltaPath, config.deltaTxnAppId, hook)

    try {
      val source = TransactionKafkaSource.read(spark, config.source)
      val validated = TransactionIngestionTransformer.decode(
        source,
        config.source.schemaRegistryUrl
      )
      val query = validated.writeStream
        .queryName("delta-transaction-ingestion")
        .option("checkpointLocation", config.source.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: Dataset[ValidatedTransactionRecord], batchId: Long) =>
          sink.writeBatch(batch, batchId)
        )
        .start()

      query.awaitTermination()
      println(
        s"Delta ingestion completed: path=${config.deltaPath} txnAppId=${config.deltaTxnAppId}"
      )
    } finally {
      spark.stop()
    }
  }

  private def parseArguments(args: Array[String]): Either[String, DeltaIngestionConfig] = {
    val parsed = args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (parts(0).nonEmpty && parts(1).nonEmpty) Right(values.updated(parts(0), parts(1)))
        else Left(s"Invalid argument: $argument")
      case (Right(_), argument) => Left(s"Expected --name=value argument, received: $argument")
      case (left @ Left(_), _)  => left
    }

    parsed.flatMap { values =>
      for {
        _ <- Either.cond(
          values.keySet.subsetOf(SupportedArguments),
          (),
          s"Unsupported arguments: ${(values.keySet -- SupportedArguments).toVector.sorted.mkString(", ")}"
        )
        failAfter <- parseOptionalBatchId(values.get("fail-after-delta-batch-id"))
      } yield DeltaIngestionConfig(
        source = TransactionIngestionConfig(
          bootstrapServers = values.getOrElse(
            "bootstrap-servers",
            TransactionIngestionConfig.DefaultBootstrapServers
          ),
          schemaRegistryUrl = values.getOrElse(
            "schema-registry-url",
            TransactionIngestionConfig.DefaultSchemaRegistryUrl
          ),
          topic = values.getOrElse("topic", TransactionIngestionConfig.DefaultTopic),
          checkpointLocation = values.getOrElse(
            "checkpoint-location",
            DeltaIngestionConfig.DefaultCheckpointLocation
          ),
          startingOffsets = values.getOrElse(
            "starting-offsets",
            TransactionIngestionConfig.DefaultStartingOffsets
          ),
          sparkMaster = values.getOrElse(
            "spark-master",
            TransactionIngestionConfig.DefaultSparkMaster
          )
        ),
        deltaPath = values.getOrElse("delta-path", DeltaIngestionConfig.DefaultDeltaPath),
        deltaTxnAppId = values.getOrElse(
          "delta-txn-app-id",
          DeltaIngestionConfig.DefaultTxnAppId
        ),
        failAfterDeltaBatchId = failAfter
      )
    }
  }

  private def parseOptionalBatchId(value: Option[String]): Either[String, Option[Long]] =
    value match {
      case None      => Right(None)
      case Some(raw) =>
        try {
          val parsed = raw.toLong
          Either.cond(
            parsed >= 0L,
            Some(parsed),
            "fail-after-delta-batch-id must be non-negative"
          )
        } catch {
          case _: NumberFormatException =>
            Left(s"fail-after-delta-batch-id must be an integer, received: $raw")
        }
    }

  private val SupportedArguments = Set(
    "bootstrap-servers",
    "schema-registry-url",
    "topic",
    "checkpoint-location",
    "starting-offsets",
    "spark-master",
    "delta-path",
    "delta-txn-app-id",
    "fail-after-delta-batch-id"
  )
}
