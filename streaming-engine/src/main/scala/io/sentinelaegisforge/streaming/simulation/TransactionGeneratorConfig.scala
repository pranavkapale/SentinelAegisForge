package io.sentinelaegisforge.streaming.simulation

import java.time.{Duration, Instant}

final case class TransactionGeneratorConfig(
    seed: Long,
    baseEventTime: Instant,
    eventSpacing: Duration = Duration.ofSeconds(30),
    ingestionDelay: Duration = Duration.ofSeconds(2)
) {
  require(baseEventTime != null, "baseEventTime must be defined")
  require(!eventSpacing.isNegative, "eventSpacing must not be negative")
  require(!ingestionDelay.isNegative, "ingestionDelay must not be negative")
}
