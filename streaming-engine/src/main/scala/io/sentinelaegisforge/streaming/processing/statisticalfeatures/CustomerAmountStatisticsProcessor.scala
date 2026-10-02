package io.sentinelaegisforge.streaming.processing.statisticalfeatures

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

import io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionCustomerFeatures

/** Scores against prior transport-order observations, then updates one Welford value per key. */
final class CustomerAmountStatisticsProcessor(inactivityTimeout: Duration)
    extends StatefulProcessor[
      CustomerCurrencyKey,
      TransactionCustomerFeatures,
      TransactionStatisticalFeatures
    ] {
  import CustomerAmountStatisticsProcessor._

  require(!inactivityTimeout.isZero && !inactivityTimeout.isNegative)
  private val timeoutMicros = Math.addExact(
    Math.multiplyExact(inactivityTimeout.getSeconds, 1000000L),
    inactivityTimeout.getNano / 1000L
  )
  require(timeoutMicros > 0L, "inactivityTimeout must be at least one microsecond")

  @transient private var statistics: ValueState[CustomerAmountStatistics] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    require(timeMode == TimeMode.EventTime(), "Statistical features require event-time mode")
    statistics = getHandle.getValueState(
      StateName,
      Encoders.product[CustomerAmountStatistics],
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: CustomerCurrencyKey,
      inputRows: Iterator[TransactionCustomerFeatures],
      timerValues: TimerValues
  ): Iterator[TransactionStatisticalFeatures] = {
    val inputs = inputRows.toVector.sortBy(input => (input.kafkaPartition, input.kafkaOffset))
    if (inputs.isEmpty) return Iterator.empty

    val previous = if (statistics.exists()) Some(statistics.get()) else None
    var current = previous
    val output = Vector.newBuilder[TransactionStatisticalFeatures]
    inputs.foreach { input =>
      require(
        input.customerId == key.customerId && input.currency == key.currency,
        "group key must match customerId and currency"
      )
      current.foreach { state =>
        require(
          input.kafkaPartition == state.observedKafkaPartition,
          s"Kafka partition lineage changed for customer-currency $key: ${state.observedKafkaPartition} -> ${input.kafkaPartition}"
        )
        require(
          input.kafkaOffset > state.lastObservedKafkaOffset,
          s"Kafka offset did not advance for customer-currency $key in partition ${input.kafkaPartition}"
        )
      }
      val amount = finite(input.amount.doubleValue(), "current amount")
      val priorCount = current.map(_.count).getOrElse(0L)
      val priorMean = current.map(state => finite(state.mean, "prior mean"))
      val priorStddev = current.filter(_.count >= 2L).map { state =>
        val variance = checkedVariance(state.m2 / (state.count - 1L).toDouble)
        finite(math.sqrt(variance), "prior standard deviation")
      }
      val zscore = for {
        mean <- priorMean
        stddev <- priorStddev if stddev > StandardDeviationFloor
      } yield finite((amount - mean) / stddev, "amount z-score")
      val status =
        if (priorCount == 0L) StatisticalFeatureStatus.NoHistory
        else if (priorCount == 1L) StatisticalFeatureStatus.InsufficientVarianceHistory
        else if (zscore.isEmpty) StatisticalFeatureStatus.ZeroVariance
        else StatisticalFeatureStatus.Ready
      output += TransactionStatisticalFeatures(
        input,
        priorCount,
        priorMean,
        priorStddev,
        zscore,
        status.externalName
      )

      val eventMicros = epochMicros(input.eventTime)
      val n1 = Math.addExact(priorCount, 1L)
      val oldMean = priorMean.getOrElse(0.0)
      val delta = amount - oldMean
      val mean1 = finite(oldMean + delta / n1.toDouble, "updated mean")
      val m2 = finite(
        current.map(_.m2).getOrElse(0.0) + delta * (amount - mean1),
        "updated M2"
      )
      checkedVariance(m2 / math.max(n1 - 1L, 1L).toDouble)
      current = Some(
        CustomerAmountStatistics(
          count = n1,
          mean = mean1,
          m2 = m2,
          latestEventTimeMicros = current
            .map(state => math.max(state.latestEventTimeMicros, eventMicros))
            .getOrElse(eventMicros),
          expiryTimerMs = current.map(_.expiryTimerMs).getOrElse(0L),
          observedKafkaPartition = input.kafkaPartition,
          lastObservedKafkaOffset = input.kafkaOffset
        )
      )
    }

    val updated = current.get
    val latestAdvanced = previous.forall(_.latestEventTimeMicros < updated.latestEventTimeMicros)
    val timer = if (latestAdvanced) {
      val replacement = ceilMillis(Math.addExact(updated.latestEventTimeMicros, timeoutMicros))
      previous.foreach(state => getHandle.deleteTimer(state.expiryTimerMs))
      getHandle.registerTimer(replacement)
      replacement
    } else previous.get.expiryTimerMs
    statistics.update(updated.copy(expiryTimerMs = timer))
    output.result().iterator
  }

  override def handleExpiredTimer(
      key: CustomerCurrencyKey,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[TransactionStatisticalFeatures] = {
    if (
      statistics.exists() &&
      statistics.get().expiryTimerMs == expiredTimerInfo.getExpiryTimeInMs()
    ) statistics.clear()
    Iterator.empty
  }
}

object CustomerAmountStatisticsProcessor {
  val StateName: String = "customerAmountStatistics"
  // Numerical guard only, not a business minimum-history or anomaly threshold.
  val VarianceNegativeEpsilon: Double = 1e-12
  val StandardDeviationFloor: Double = 1e-12

  private[statisticalfeatures] def finite(value: Double, name: String): Double = {
    require(!value.isNaN && !value.isInfinity, s"Non-finite $name")
    value
  }

  private[statisticalfeatures] def checkedVariance(variance: Double): Double = {
    finite(variance, "variance")
    if (variance < -VarianceNegativeEpsilon)
      throw new IllegalStateException(s"Materially negative variance: $variance")
    math.max(variance, 0.0)
  }

  private def epochMicros(timestamp: Timestamp): Long = {
    val instant = timestamp.toInstant
    require(instant.getNano % 1000 == 0, "Event timestamp must have microsecond precision")
    Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
  }

  private def ceilMillis(micros: Long): Long = {
    val floor = Math.floorDiv(micros, 1000L)
    if (Math.floorMod(micros, 1000L) == 0L) floor else Math.addExact(floor, 1L)
  }
}
