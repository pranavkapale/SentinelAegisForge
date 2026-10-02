package io.sentinelaegisforge.streaming.processing.rollingfeatures

import java.math.RoundingMode
import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{
  ExpiredTimerInfo,
  MapState,
  OutputMode,
  StatefulProcessor,
  TTLConfig,
  TimeMode,
  TimerValues,
  ValueState
}

/** Prior-only event-time features with one cleanup timer per active customer. */
final class RollingFeatureProcessor
    extends StatefulProcessor[String, RollingFeatureInput, TransactionCustomerFeatures] {
  import RollingFeatureProcessor._

  @transient private var rollingEvents: MapState[String, RollingCustomerEvent] = _
  @transient private var nextCleanupTimerMs: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    require(timeMode == TimeMode.EventTime(), "Rolling features require event-time mode")
    rollingEvents = getHandle.getMapState(
      RollingEventsStateName,
      Encoders.STRING,
      Encoders.product[RollingCustomerEvent],
      TTLConfig.NONE
    )
    nextCleanupTimerMs = getHandle.getValueState(
      NextCleanupTimerStateName,
      Encoders.scalaLong,
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[RollingFeatureInput],
      timerValues: TimerValues
  ): Iterator[TransactionCustomerFeatures] = {
    val inputs = inputRows.toVector.sortBy(input => (epochMicros(input.eventTime), input.eventId))
    inputs.foreach(input => require(input.customerId == key, "group key must match customerId"))

    val output = Vector.newBuilder[TransactionCustomerFeatures]
    inputs.groupBy(input => epochMicros(input.eventTime)).toVector.sortBy(_._1).foreach {
      case (currentMicros, peers) =>
        val history = rollingEvents.iterator().toVector.map(_._2)
        val countLower = Math.subtractExact(currentMicros, CountWindowMicros)
        val amountLower = Math.subtractExact(currentMicros, AmountWindowMicros)
        val priorCount = history.iterator
          .filter(item =>
            item.eventTimeMicros >= countLower && item.eventTimeMicros < currentMicros
          )
          .foldLeft(0L)((count, _) => Math.addExact(count, 1L))
        // All same-time peers read the same prior history before any peer enters state.
        peers.foreach { input =>
          val priorAmount = history.iterator
            .filter(item =>
              item.eventTimeMicros >= amountLower && item.eventTimeMicros < currentMicros &&
                item.currency == input.currency
            )
            .foldLeft(ZeroAmount)((sum, item) => checkedSum(sum.add(item.amount)))
          output += TransactionCustomerFeatures.from(input, priorCount, priorAmount)
        }
        peers.foreach { input =>
          rollingEvents.updateValue(
            input.eventId,
            RollingCustomerEvent(currentMicros, input.amount, input.currency)
          )
        }
    }

    if (inputs.nonEmpty) scheduleEarliestCleanup()
    output.result().iterator
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[TransactionCustomerFeatures] = {
    val firedAt = expiredTimerInfo.getExpiryTimeInMs()
    if (nextCleanupTimerMs.exists() && nextCleanupTimerMs.get() == firedAt) {
      // Spark may advance the watermark past several expiries before invoking this timer.
      // Clear every entry already due, rather than requiring another micro-batch per entry.
      val expiredThrough = math.max(firedAt, timerValues.getCurrentWatermarkInMs())
      rollingEvents.iterator().toVector.foreach { case (eventId, event) =>
        if (cleanupTimerMillis(event.eventTimeMicros) <= expiredThrough) {
          rollingEvents.removeKey(eventId)
        }
      }
      nextCleanupTimerMs.clear()
      scheduleEarliestCleanup()
    }
    Iterator.empty
  }

  private def scheduleEarliestCleanup(): Unit = {
    val earliest = rollingEvents
      .iterator()
      .map { case (_, event) =>
        cleanupTimerMillis(event.eventTimeMicros)
      }
      .reduceOption(_ min _)
    val previous = if (nextCleanupTimerMs.exists()) Some(nextCleanupTimerMs.get()) else None
    if (earliest != previous) {
      previous.foreach(getHandle.deleteTimer)
      earliest match {
        case Some(next) =>
          getHandle.registerTimer(next)
          nextCleanupTimerMs.update(next)
        case None => nextCleanupTimerMs.clear()
      }
    }
  }
}

object RollingFeatureProcessor {
  val RollingEventsStateName: String = "rollingEvents"
  val NextCleanupTimerStateName: String = "nextCleanupTimerMs"
  val CountWindowMicros: Long = 5L * 60L * 1000000L
  val AmountWindowMicros: Long = 10L * 60L * 1000000L
  val AmountPrecision: Int = 38
  val AmountScale: Int = 4

  private val ZeroAmount = new java.math.BigDecimal("0.0000")

  private[rollingfeatures] def checkedSum(value: java.math.BigDecimal): java.math.BigDecimal = {
    val exact = value.setScale(AmountScale, RoundingMode.UNNECESSARY)
    if (exact.precision() > AmountPrecision) {
      throw new ArithmeticException(s"Rolling sum exceeds decimal(38,4): $value")
    }
    exact
  }

  private[rollingfeatures] def epochMicros(timestamp: Timestamp): Long = {
    val instant = timestamp.toInstant
    require(instant.getNano % 1000 == 0, "Event timestamp must have microsecond precision")
    Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
  }

  private[rollingfeatures] def cleanupTimerMillis(eventMicros: Long): Long = {
    val expiryMicros = Math.addExact(eventMicros, AmountWindowMicros)
    val floor = Math.floorDiv(expiryMicros, 1000L)
    if (Math.floorMod(expiryMicros, 1000L) == 0L) floor else Math.addExact(floor, 1L)
  }
}
