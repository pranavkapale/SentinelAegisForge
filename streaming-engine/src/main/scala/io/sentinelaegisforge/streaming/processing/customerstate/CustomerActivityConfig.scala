package io.sentinelaegisforge.streaming.processing.customerstate

import java.time.Duration

import scala.util.Try

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionConfig
import io.sentinelaegisforge.streaming.processing.deduplication.TransactionDeduplicationConfig

final case class CustomerActivityConfig(
    deduplicatedDeltaPath: String,
    snapshotDeltaPath: String,
    checkpointLocation: String,
    deltaTxnAppId: String,
    watermarkDelay: String,
    inactivityTimeout: Duration,
    sparkMaster: String
) {
  require(deduplicatedDeltaPath.trim.nonEmpty, "deduplicatedDeltaPath must not be blank")
  require(snapshotDeltaPath.trim.nonEmpty, "snapshotDeltaPath must not be blank")
  require(checkpointLocation.trim.nonEmpty, "checkpointLocation must not be blank")
  require(deltaTxnAppId.trim.nonEmpty, "deltaTxnAppId must not be blank")
  require(watermarkDelay.trim.nonEmpty, "watermarkDelay must not be blank")
  require(!inactivityTimeout.isZero && !inactivityTimeout.isNegative)
  require(sparkMaster.trim.nonEmpty, "sparkMaster must not be blank")
}

object CustomerActivityConfig {
  val DefaultDeduplicatedDeltaPath: String =
    TransactionDeduplicationConfig.DefaultDeduplicatedDeltaPath
  val DefaultSnapshotDeltaPath: String = ".local/delta/customer_activity_snapshots"
  val DefaultCheckpointLocation: String = ".local/checkpoints/customer-activity-state"
  val DefaultTxnAppId: String = "sentinel-customer-activity-snapshots-v1"
  val DefaultWatermarkDelay: String = "10 minutes"
  val DefaultInactivityTimeout: Duration = Duration.ofHours(24L)
  val DefaultSparkMaster: String = TransactionIngestionConfig.DefaultSparkMaster

  val StateStoreProviderConfig: String = "spark.sql.streaming.stateStore.providerClass"
  val RequiredStateStoreProvider: String =
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
}

object DurationArgument {
  private val DurationPattern =
    "(?i)^([1-9][0-9]*)\\s*(ms|millisecond|milliseconds|second|seconds|minute|minutes|hour|hours|day|days)$".r

  def parse(value: String): Either[String, Duration] = value.trim match {
    case DurationPattern(quantityText, unit) =>
      Try(quantityText.toLong).toEither.left
        .map(_ => s"Duration quantity is too large: '$quantityText'")
        .map { quantity =>
          unit.toLowerCase match {
            case "ms" | "millisecond" | "milliseconds" => Duration.ofMillis(quantity)
            case "second" | "seconds"                  => Duration.ofSeconds(quantity)
            case "minute" | "minutes"                  => Duration.ofMinutes(quantity)
            case "hour" | "hours"                      => Duration.ofHours(quantity)
            case "day" | "days"                        => Duration.ofDays(quantity)
          }
        }
    case _ => Left(s"Invalid duration '$value'; expected a positive value such as '24 hours'")
  }
}
