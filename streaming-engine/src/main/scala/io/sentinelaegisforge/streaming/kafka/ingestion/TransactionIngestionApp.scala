package io.sentinelaegisforge.streaming.kafka.ingestion

import org.apache.spark.sql.streaming.Trigger

object TransactionIngestionApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession.create(config.sparkMaster)
    spark.sparkContext.setLogLevel("WARN")

    try {
      val source = TransactionKafkaSource.read(spark, config)
      val validated = TransactionIngestionTransformer.decode(source, config.schemaRegistryUrl)
      val sink = new DiagnosticTransactionSink()
      val query = validated.writeStream
        .queryName("transaction-ingestion-diagnostic")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch, batchId) => sink.process(batch, batchId))
        .start()

      query.awaitTermination()
      println(s"Transaction ingestion result: processed=${sink.totalProcessedRows}")
    } finally {
      spark.stop()
    }
  }

  private def parseArguments(args: Array[String]): Either[String, TransactionIngestionConfig] = {
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
        TransactionIngestionConfig(
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
            TransactionIngestionConfig.DefaultCheckpointLocation
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
        s"Unsupported arguments: ${(values.keySet -- SupportedArguments).toVector.sorted.mkString(", ")}"
      )
    }
  }

  private val SupportedArguments = Set(
    "bootstrap-servers",
    "schema-registry-url",
    "topic",
    "checkpoint-location",
    "starting-offsets",
    "spark-master"
  )
}
