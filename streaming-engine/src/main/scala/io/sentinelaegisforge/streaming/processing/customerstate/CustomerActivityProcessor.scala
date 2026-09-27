package io.sentinelaegisforge.streaming.processing.customerstate

import java.math.RoundingMode
import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{
  ExpiredTimerInfo,
  OutputMode,
  StatefulProcessor,
  TTLConfig,
  TimeMode,
  TimerValues,
  ValueState
}

/** Customer-keyed state machine with explicit event-time inactivity timers.
  *
  * TTL is deliberately disabled. The stored timer timestamp is part of state so an obsolete timer
  * cannot clear newer state.
  */
final class CustomerActivityProcessor(inactivityTimeout: Duration)
    extends StatefulProcessor[String, CustomerActivityInput, CustomerActivitySnapshot] {
  import CustomerActivityProcessor._

  require(!inactivityTimeout.isZero, "inactivityTimeout must be positive")
  require(!inactivityTimeout.isNegative, "inactivityTimeout must be positive")

  @transient private var customerActivity: ValueState[CustomerActivityState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    require(timeMode == TimeMode.EventTime(), "Customer activity requires event-time mode")
    customerActivity = getHandle.getValueState(
      StateName,
      Encoders.product[CustomerActivityState],
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[CustomerActivityInput],
      timerValues: TimerValues
  ): Iterator[CustomerActivitySnapshot] = {
    var batchCount = 0L
    var batchTotal = ZeroAmount
    var batchFirst: Timestamp = null
    var batchLatest: Timestamp = null

    inputRows.foreach { input =>
      require(input.customer_id == key, "grouping key must match customer_id")
      batchCount = Math.addExact(batchCount, 1L)
      batchTotal = checkedAmount(batchTotal.add(input.amount))
      batchFirst = earlier(batchFirst, input.event_time)
      batchLatest = later(batchLatest, input.event_time)
    }

    if (batchCount == 0L) {
      Iterator.empty
    } else {
      val previous = if (customerActivity.exists()) Some(customerActivity.get()) else None
      val stateCount = Math.addExact(previous.map(_.eventCount).getOrElse(0L), batchCount)
      val stateTotal = checkedAmount(
        previous.map(_.amountTotal).getOrElse(ZeroAmount).add(batchTotal)
      )
      val firstEventTime = previous
        .map(state => earlier(state.firstEventTime, batchFirst))
        .getOrElse(batchFirst)
      val latestEventTime = previous
        .map(state => later(state.latestEventTime, batchLatest))
        .getOrElse(batchLatest)
      val latestAdvanced = previous.forall(_.latestEventTime.before(latestEventTime))
      val expiryTimerTimestamp = if (latestAdvanced) {
        val replacement = Math.addExact(
          latestEventTime.toInstant.toEpochMilli,
          inactivityTimeout.toMillis
        )
        previous.foreach(state => getHandle.deleteTimer(state.expiryTimerTimestamp))
        getHandle.registerTimer(replacement)
        replacement
      } else {
        previous
          .map(_.expiryTimerTimestamp)
          .getOrElse(
            throw new IllegalStateException("New customer state must establish an expiry timer")
          )
      }

      val updated = CustomerActivityState(
        eventCount = stateCount,
        amountTotal = stateTotal,
        firstEventTime = firstEventTime,
        latestEventTime = latestEventTime,
        expiryTimerTimestamp = expiryTimerTimestamp
      )
      customerActivity.update(updated)

      Iterator.single(
        snapshot(
          key,
          CustomerActivityLifecycle.Updated,
          batchCount,
          batchTotal,
          updated
        )
      )
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[CustomerActivitySnapshot] = {
    val expiredAt = expiredTimerInfo.getExpiryTimeInMs()
    if (customerActivity.exists()) {
      val current = customerActivity.get()
      if (current.expiryTimerTimestamp == expiredAt) {
        customerActivity.clear()
        Iterator.single(
          snapshot(
            key,
            CustomerActivityLifecycle.Expired,
            batchEventCount = 0L,
            batchAmountTotal = ZeroAmount,
            current
          )
        )
      } else {
        Iterator.empty
      }
    } else {
      Iterator.empty
    }
  }

  private def snapshot(
      customerId: String,
      lifecycle: CustomerActivityLifecycle,
      batchEventCount: Long,
      batchAmountTotal: java.math.BigDecimal,
      state: CustomerActivityState
  ): CustomerActivitySnapshot =
    CustomerActivitySnapshot(
      customerId = customerId,
      lifecycleType = lifecycle.externalName,
      batchEventCount = batchEventCount,
      batchAmountTotal = batchAmountTotal,
      stateEventCount = state.eventCount,
      stateAmountTotal = state.amountTotal,
      stateFirstEventTime = state.firstEventTime,
      stateLatestEventTime = state.latestEventTime,
      stateExpiryTime = new Timestamp(state.expiryTimerTimestamp)
    )
}

object CustomerActivityProcessor {
  val StateName: String = "customerActivity"
  val AmountPrecision: Int = 38
  val AmountScale: Int = 18

  private val ZeroAmount = new java.math.BigDecimal("0").setScale(AmountScale)

  private[customerstate] def checkedAmount(
      value: java.math.BigDecimal
  ): java.math.BigDecimal = {
    val normalized = value.setScale(AmountScale, RoundingMode.UNNECESSARY)
    if (normalized.precision() > AmountPrecision) {
      throw new ArithmeticException(
        s"Customer amount total exceeds decimal($AmountPrecision,$AmountScale): $value"
      )
    }
    normalized
  }

  private def earlier(left: Timestamp, right: Timestamp): Timestamp =
    if (left == null || right.before(left)) right else left

  private def later(left: Timestamp, right: Timestamp): Timestamp =
    if (left == null || right.after(left)) right else left
}
