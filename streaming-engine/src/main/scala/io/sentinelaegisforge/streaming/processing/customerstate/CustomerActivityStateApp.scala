package io.sentinelaegisforge.streaming.processing.customerstate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.CustomerActivityDeltaSink

object CustomerActivityStateApp {
  def main(args: Array[String]): Unit = {
    val config = parseArguments(args).fold(
      message => throw new IllegalArgumentException(message),
      identity
    )
    val spark = TransactionSparkSession
      .builder(config.sparkMaster)
      .config(
        CustomerActivityConfig.StateStoreProviderConfig,
        CustomerActivityConfig.RequiredStateStoreProvider
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      require(
        DeltaTable.isDeltaTable(spark, config.deduplicatedDeltaPath),
        s"No deduplicated Delta table exists at ${config.deduplicatedDeltaPath}"
      )

      val source = spark.readStream
        .format("delta")
        .load(config.deduplicatedDeltaPath)
      val snapshots = CustomerActivityTransformer.transform(
        source,
        config.watermarkDelay,
        config.inactivityTimeout
      )
      val sink = new CustomerActivityDeltaSink(
        config.snapshotDeltaPath,
        config.deltaTxnAppId
      )
      val query = snapshots.writeStream
        .queryName("customer-activity-state")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: Dataset[CustomerActivitySnapshot], batchId: Long) =>
          sink.writeBatch(batch, batchId)
        )
        .start()

      query.awaitTermination()
      val progress = query.recentProgress.toVector.map(CustomerActivityProgress.from)
      CustomerActivityProgressReporter.report(progress)
      println(
        s"Customer activity state completed: source=${config.deduplicatedDeltaPath} target=${config.snapshotDeltaPath} txnAppId=${config.deltaTxnAppId} watermarkDelay=${config.watermarkDelay} inactivityTimeout=${config.inactivityTimeout} stateStore=${CustomerActivityConfig.RequiredStateStoreProvider}"
      )
    } finally {
      spark.stop()
    }
  }

  private[customerstate] def parseArguments(
      args: Array[String]
  ): Either[String, CustomerActivityConfig] = {
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
      if (unsupported.nonEmpty) {
        Left(s"Unsupported arguments: ${unsupported.toVector.sorted.mkString(", ")}")
      } else {
        DurationArgument
          .parse(
            values.getOrElse(
              "inactivity-timeout",
              sys.env.getOrElse("CUSTOMER_STATE_INACTIVITY", "24 hours")
            )
          )
          .map { inactivityTimeout =>
            CustomerActivityConfig(
              deduplicatedDeltaPath = values.getOrElse(
                "deduplicated-delta-path",
                CustomerActivityConfig.DefaultDeduplicatedDeltaPath
              ),
              snapshotDeltaPath = values.getOrElse(
                "snapshot-delta-path",
                CustomerActivityConfig.DefaultSnapshotDeltaPath
              ),
              checkpointLocation = values.getOrElse(
                "checkpoint-location",
                CustomerActivityConfig.DefaultCheckpointLocation
              ),
              deltaTxnAppId = values.getOrElse(
                "delta-txn-app-id",
                CustomerActivityConfig.DefaultTxnAppId
              ),
              watermarkDelay = values.getOrElse(
                "watermark-delay",
                sys.env.getOrElse(
                  "WATERMARK_DELAY",
                  CustomerActivityConfig.DefaultWatermarkDelay
                )
              ),
              inactivityTimeout = inactivityTimeout,
              sparkMaster = values.getOrElse(
                "spark-master",
                CustomerActivityConfig.DefaultSparkMaster
              )
            )
          }
      }
    }
  }

  private val SupportedArguments = Set(
    "deduplicated-delta-path",
    "snapshot-delta-path",
    "checkpoint-location",
    "delta-txn-app-id",
    "watermark-delay",
    "inactivity-timeout",
    "spark-master"
  )
}
