package io.sentinelaegisforge.streaming.simulation

import java.time.Instant
import java.time.temporal.ChronoUnit

/** Bounded local scenario contract; no runtime clock or model/risk output is an input. */
final case class SyntheticScenarioConfig(
    seed: Long,
    baseTime: Instant,
    transactionCount: Int = 1500,
    customerCount: Int = 90,
    timeHorizonDays: Int = 9,
    usdWeightPercent: Int = 65,
    behaviorProfile: String = "mixed-v1",
    labelDelayProfile: String = "short-long-v1"
) {
  require(baseTime != null && baseTime == baseTime.truncatedTo(ChronoUnit.MICROS))
  require(transactionCount >= 30 && transactionCount <= 2000)
  require(customerCount >= 2 && customerCount <= 300 && customerCount <= transactionCount)
  require(timeHorizonDays >= 3 && timeHorizonDays <= 30)
  require(usdWeightPercent >= 1 && usdWeightPercent <= 99)
  require(behaviorProfile == "mixed-v1")
  require(labelDelayProfile == "short-long-v1")
}

object SyntheticScenarioConfig {
  val ContractVersion: String = "synthetic-fraud-scenario-v1"
  val WaveCount: Int = 3
}
