package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.TransactionStatisticalFeatureDeltaSink
import io.sentinelaegisforge.streaming.processing.customerstate.DurationArgument

object TransactionStatisticalFeatureApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession
      .builder(config.sparkMaster)
      .config(
        StatisticalFeatureConfig.StateStoreProviderConfig,
        StatisticalFeatureConfig.RequiredStateStoreProvider
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      require(
        DeltaTable.isDeltaTable(spark, config.sourceDeltaPath),
        s"No customer-feature Delta table exists at ${config.sourceDeltaPath}"
      )
      val source = spark.readStream.format("delta").load(config.sourceDeltaPath)
      val features = StatisticalFeatureTransformer.transform(
        source,
        config.watermarkDelay,
        config.inactivityTimeout
      )
      val sink = new TransactionStatisticalFeatureDeltaSink(
        config.targetDeltaPath,
        config.deltaTxnAppId
      )
      val query = features.writeStream
        .queryName("transaction-customer-statistical-features")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: Dataset[TransactionStatisticalFeatures], batchId: Long) =>
          sink.writeBatch(batch, batchId)
        )
        .start()

      query.awaitTermination()
      StatisticalFeatureProgress.report(query.recentProgress.toVector)
      println(
        s"Statistical features completed: source=${config.sourceDeltaPath} target=${config.targetDeltaPath} checkpoint=${config.checkpointLocation} txnAppId=${config.deltaTxnAppId} watermarkDelay=${config.watermarkDelay} inactivityTimeout=${config.inactivityTimeout} stateStore=${StatisticalFeatureConfig.RequiredStateStoreProvider}"
      )
    } finally {
      spark.stop()
    }
  }

  private[statisticalfeatures] def parseArguments(
      args: Array[String]
  ): Either[String, StatisticalFeatureConfig] = {
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
        DurationArgument
          .parse(
            values.getOrElse(
              "inactivity-timeout",
              sys.env.getOrElse("STATISTICAL_FEATURE_INACTIVITY", "24 hours")
            )
          )
          .map { inactivityTimeout =>
            StatisticalFeatureConfig(
              sourceDeltaPath = values.getOrElse(
                "source-delta-path",
                StatisticalFeatureConfig.DefaultSourceDeltaPath
              ),
              targetDeltaPath = values.getOrElse(
                "target-delta-path",
                StatisticalFeatureConfig.DefaultTargetDeltaPath
              ),
              checkpointLocation = values.getOrElse(
                "checkpoint-location",
                StatisticalFeatureConfig.DefaultCheckpointLocation
              ),
              deltaTxnAppId = values.getOrElse(
                "delta-txn-app-id",
                StatisticalFeatureConfig.DefaultTxnAppId
              ),
              watermarkDelay = values.getOrElse(
                "watermark-delay",
                sys.env.getOrElse(
                  "WATERMARK_DELAY",
                  StatisticalFeatureConfig.DefaultWatermarkDelay
                )
              ),
              inactivityTimeout = inactivityTimeout,
              sparkMaster =
                values.getOrElse("spark-master", StatisticalFeatureConfig.DefaultSparkMaster)
            )
          }
    }
  }

  private val SupportedArguments = Set(
    "source-delta-path",
    "target-delta-path",
    "checkpoint-location",
    "delta-txn-app-id",
    "watermark-delay",
    "inactivity-timeout",
    "spark-master"
  )
}
