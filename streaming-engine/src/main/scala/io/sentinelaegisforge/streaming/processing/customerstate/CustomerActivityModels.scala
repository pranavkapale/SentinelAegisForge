package io.sentinelaegisforge.streaming.processing.customerstate

import java.sql.Timestamp

/** Minimal typed input projected from the deduplicated transaction table. */
final case class CustomerActivityInput(
    customer_id: String,
    event_time: Timestamp,
    amount: java.math.BigDecimal
)

/** Constant-sized per-customer state retained by transformWithState. */
final case class CustomerActivityState(
    eventCount: Long,
    amountTotal: java.math.BigDecimal,
    firstEventTime: Timestamp,
    latestEventTime: Timestamp,
    expiryTimerTimestamp: Long
)

sealed abstract class CustomerActivityLifecycle(val externalName: String)

object CustomerActivityLifecycle {
  case object Updated extends CustomerActivityLifecycle("UPDATED")
  case object Expired extends CustomerActivityLifecycle("EXPIRED")

  val supported: Set[String] = Set(Updated.externalName, Expired.externalName)
}

/** Append-only lifecycle observation emitted by the customer state machine. */
final case class CustomerActivitySnapshot(
    customerId: String,
    lifecycleType: String,
    batchEventCount: Long,
    batchAmountTotal: java.math.BigDecimal,
    stateEventCount: Long,
    stateAmountTotal: java.math.BigDecimal,
    stateFirstEventTime: Timestamp,
    stateLatestEventTime: Timestamp,
    stateExpiryTime: Timestamp
)
