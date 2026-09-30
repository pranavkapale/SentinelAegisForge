package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionCustomerFeatures

/** Constant-sized, customer-keyed Welford state for the current inactivity lifecycle. */
final case class CustomerAmountStatistics(
    count: Long,
    mean: Double,
    m2: Double,
    latestEventTimeMicros: Long,
    expiryTimerMs: Long,
    observedKafkaPartition: Int,
    lastObservedKafkaOffset: Long
) {
  require(count > 0L, "Statistical state count must be positive")
  require(!mean.isNaN && !mean.isInfinity, "Statistical state mean must be finite")
  require(!m2.isNaN && !m2.isInfinity, "Statistical state M2 must be finite")
}

sealed trait StatisticalFeatureStatus {
  def externalName: String
}

object StatisticalFeatureStatus {
  case object NoHistory extends StatisticalFeatureStatus {
    val externalName = "NO_HISTORY"
  }
  case object InsufficientVarianceHistory extends StatisticalFeatureStatus {
    val externalName = "INSUFFICIENT_VARIANCE_HISTORY"
  }
  case object ZeroVariance extends StatisticalFeatureStatus {
    val externalName = "ZERO_VARIANCE"
  }
  case object Ready extends StatisticalFeatureStatus {
    val externalName = "READY"
  }
}

/** The nested base record keeps the unchanged Phase 9 context together in the Spark encoder. */
final case class TransactionStatisticalFeatures(
    base: TransactionCustomerFeatures,
    priorAmountObservationCount: Long,
    priorAmountMean: Option[Double],
    priorAmountStddev: Option[Double],
    amountZscore: Option[Double],
    statisticalFeatureStatus: String
) {
  require(priorAmountObservationCount >= 0L)
  Vector(priorAmountMean, priorAmountStddev, amountZscore).flatten.foreach { value =>
    require(!value.isNaN && !value.isInfinity, "Statistical output values must be finite")
  }
}
