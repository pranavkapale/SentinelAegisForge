package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import java.time.Duration

import io.sentinelaegisforge.streaming.processing.customerstate.CustomerActivityConfig
import io.sentinelaegisforge.streaming.processing.rollingfeatures.RollingFeatureConfig

final case class StatisticalFeatureConfig(
    sourceDeltaPath: String,
    targetDeltaPath: String,
    checkpointLocation: String,
    deltaTxnAppId: String,
    watermarkDelay: String,
    inactivityTimeout: Duration,
    sparkMaster: String
) {
  require(sourceDeltaPath.trim.nonEmpty)
  require(targetDeltaPath.trim.nonEmpty)
  require(checkpointLocation.trim.nonEmpty)
  require(deltaTxnAppId.trim.nonEmpty)
  require(watermarkDelay.trim.nonEmpty)
  require(!inactivityTimeout.isZero && !inactivityTimeout.isNegative)
  require(sparkMaster.trim.nonEmpty)
}

object StatisticalFeatureConfig {
  val DefaultSourceDeltaPath: String = RollingFeatureConfig.DefaultFeatureDeltaPath
  val DefaultTargetDeltaPath: String = ".local/delta/transaction_statistical_features_v2"
  val DefaultCheckpointLocation: String = ".local/checkpoints/customer-statistical-features-v2"
  val DefaultTxnAppId: String = "sentinel-transaction-statistical-features-v2"
  val DefaultWatermarkDelay: String = RollingFeatureConfig.DefaultWatermarkDelay
  val DefaultInactivityTimeout: Duration = Duration.ofHours(24L)
  val DefaultSparkMaster: String = RollingFeatureConfig.DefaultSparkMaster
  val StateStoreProviderConfig: String = CustomerActivityConfig.StateStoreProviderConfig
  val RequiredStateStoreProvider: String = CustomerActivityConfig.RequiredStateStoreProvider
}
