package io.sentinelaegisforge.streaming.processing.rollingfeatures

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.TransactionCustomerFeatureDeltaSink

object TransactionRollingFeatureApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession
      .builder(config.sparkMaster)
      .config(
        RollingFeatureConfig.StateStoreProviderConfig,
        RollingFeatureConfig.RequiredStateStoreProvider
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      require(
        DeltaTable.isDeltaTable(spark, config.deduplicatedDeltaPath),
        s"No deduplicated Delta table exists at ${config.deduplicatedDeltaPath}"
      )
      val source = spark.readStream.format("delta").load(config.deduplicatedDeltaPath)
      val features = RollingFeatureTransformer.transform(source, config.watermarkDelay)
      val sink = new TransactionCustomerFeatureDeltaSink(
        config.featureDeltaPath,
        config.deltaTxnAppId
      )
      val query = features.writeStream
        .queryName("transaction-customer-rolling-features")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: Dataset[TransactionCustomerFeatures], batchId: Long) =>
          sink.writeBatch(batch, batchId)
        )
        .start()

      query.awaitTermination()
      RollingFeatureProgressReporter.report(
        query.recentProgress.toVector.map(RollingFeatureProgress.from)
      )
      println(
        s"Rolling features completed: source=${config.deduplicatedDeltaPath} target=${config.featureDeltaPath} checkpoint=${config.checkpointLocation} txnAppId=${config.deltaTxnAppId} watermarkDelay=${config.watermarkDelay} stateStore=${RollingFeatureConfig.RequiredStateStoreProvider}"
      )
    } finally {
      spark.stop()
    }
  }

  private[rollingfeatures] def parseArguments(
      args: Array[String]
  ): Either[String, RollingFeatureConfig] = {
    val parsed = args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (parts(0).nonEmpty && parts(1).nonEmpty) Right(values.updated(parts(0), parts(1)))
        else Left(s"Invalid argument: $argument")
      case (Right(_), argument) => Left(s"Expected --name=value argument, received: $argument")
      case (left @ Left(_), _)  => left
    }
    parsed.flatMap { values =>
      val unsupported = values.keySet -- SupportedArguments
      if (unsupported.nonEmpty)
        Left(s"Unsupported arguments: ${unsupported.toVector.sorted.mkString(", ")}")
      else
        Right(
          RollingFeatureConfig(
            deduplicatedDeltaPath = values.getOrElse(
              "deduplicated-delta-path",
              RollingFeatureConfig.DefaultDeduplicatedDeltaPath
            ),
            featureDeltaPath = values.getOrElse(
              "feature-delta-path",
              RollingFeatureConfig.DefaultFeatureDeltaPath
            ),
            checkpointLocation = values.getOrElse(
              "checkpoint-location",
              RollingFeatureConfig.DefaultCheckpointLocation
            ),
            deltaTxnAppId =
              values.getOrElse("delta-txn-app-id", RollingFeatureConfig.DefaultTxnAppId),
            watermarkDelay = values.getOrElse(
              "watermark-delay",
              sys.env.getOrElse("WATERMARK_DELAY", RollingFeatureConfig.DefaultWatermarkDelay)
            ),
            sparkMaster = values.getOrElse("spark-master", RollingFeatureConfig.DefaultSparkMaster)
          )
        )
    }
  }

  private val SupportedArguments = Set(
    "deduplicated-delta-path",
    "feature-delta-path",
    "checkpoint-location",
    "delta-txn-app-id",
    "watermark-delay",
    "spark-master"
  )
}
