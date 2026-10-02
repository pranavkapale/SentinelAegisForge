package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import java.sql.Timestamp
import java.time.{Duration, Instant}

import org.apache.spark.sql.streaming.{
  ExpiredTimerInfo,
  OutputMode,
  TimeMode,
  TimerValues,
  TwsTester
}
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionCustomerFeatures

final class CustomerAmountStatisticsProcessorSpec extends AnyFunSuite {
  test("first observation has no prior history; canonical Welford sequence scores before update") {
    val tester = newTester()
    val first = tester.test(key("a"), List(input("a", 0L, "12:00", "100"))).head
    assert(first.priorAmountObservationCount == 0L)
    assert(first.priorAmountMean.isEmpty)
    assert(first.priorAmountStddev.isEmpty)
    assert(first.amountZscore.isEmpty)
    assert(first.statisticalFeatureStatus == "NO_HISTORY")
    assertState(tester, "a", 1L, 100.0, 0.0)

    val second = tester.test(key("a"), List(input("a", 1L, "12:01", "200"))).head
    assert(second.priorAmountObservationCount == 1L)
    assert(second.priorAmountMean.contains(100.0))
    assert(second.priorAmountStddev.isEmpty)
    assert(second.amountZscore.isEmpty)
    assert(second.statisticalFeatureStatus == "INSUFFICIENT_VARIANCE_HISTORY")
    assertState(tester, "a", 2L, 150.0, 5000.0)

    val third = tester.test(key("a"), List(input("a", 2L, "12:02", "300"))).head
    assert(third.priorAmountObservationCount == 2L)
    assertClose(third.priorAmountMean.get, 150.0)
    assertClose(third.priorAmountStddev.get, math.sqrt(5000.0))
    assertClose(third.amountZscore.get, 150.0 / math.sqrt(5000.0))
    assert(third.statisticalFeatureStatus == "READY")
    assertState(tester, "a", 3L, 200.0, 20000.0)
  }

  test("zero variance never creates a non-finite z-score") {
    val tester = newTester()
    (0L until 3L).foreach(offset => tester.test(key("a"), List(input("a", offset, "12:00", "100"))))
    val scored = tester.test(key("a"), List(input("a", 3L, "12:01", "150"))).head
    assert(scored.priorAmountObservationCount == 3L)
    assert(scored.priorAmountMean.contains(100.0))
    assert(scored.priorAmountStddev.contains(0.0))
    assert(scored.amountZscore.isEmpty)
    assert(scored.statisticalFeatureStatus == "ZERO_VARIANCE")
  }

  test("transport offset orders one invocation, not iterator, amount, or event time") {
    val rows = List(
      input("a", 2L, "12:02", "300"),
      input("a", 0L, "12:10", "100"),
      input("a", 1L, "12:00", "200")
    )
    val forward = newTester().test(key("a"), rows)
    val reverse = newTester().test(key("a"), rows.reverse)
    assert(forward == reverse)
    assert(forward.map(_.base.kafkaOffset) == List(0L, 1L, 2L))
    assert(forward.map(_.priorAmountObservationCount) == List(0L, 1L, 2L))
    assertClose(forward.last.priorAmountMean.get, 150.0)
  }

  test("late event is scored against already observed later event times") {
    val tester = newTester()
    tester.test(key("a"), List(input("a", 0L, "12:00", "100")))
    tester.test(key("a"), List(input("a", 1L, "12:10", "200")))
    val late = tester.test(key("a"), List(input("a", 2L, "12:05", "300"))).head
    assert(late.priorAmountObservationCount == 2L)
    assertClose(late.priorAmountMean.get, 150.0)
    assertClose(late.amountZscore.get, 150.0 / math.sqrt(5000.0))
    val state = tester
      .peekValueState[CustomerAmountStatistics](
        CustomerAmountStatisticsProcessor.StateName,
        key("a")
      )
      .get
    assert(state.latestEventTimeMicros == epochMicros("12:10"))
    assert(state.expiryTimerMs == instant("2030-01-01T13:10:00Z").toEpochMilli)
  }

  test("customers are independent and partition or offset lineage violation fails") {
    val tester = newTester()
    tester.test(key("a"), List(input("a", 0L, "12:00", "100")))
    val b = tester.test(key("b"), List(input("b", 0L, "12:00", "300"))).head
    assert(b.priorAmountObservationCount == 0L)
    assertState(tester, "a", 1L, 100.0, 0.0)
    assertState(tester, "b", 1L, 300.0, 0.0)
    intercept[IllegalArgumentException] {
      tester.test(key("a"), List(input("a", 1L, "12:01", "200").copy(kafkaPartition = 1)))
    }
    intercept[IllegalArgumentException] {
      tester.test(key("a"), List(input("a", 0L, "12:01", "200")))
    }
  }

  test("authoritative inactivity timer expires state and reactivation starts without history") {
    val tester = newTester()
    tester.test(key("a"), List(input("a", 0L, "12:00", "100")))
    val timer = state(tester, "a").expiryTimerMs
    assert(tester.setWatermark(timer).isEmpty)
    assert(
      tester
        .peekValueState[CustomerAmountStatistics](
          CustomerAmountStatisticsProcessor.StateName,
          key("a")
        )
        .isEmpty
    )
    val reactivated = tester.test(key("a"), List(input("a", 1L, "13:01", "200"))).head
    assert(reactivated.priorAmountObservationCount == 0L)
    assert(reactivated.statisticalFeatureStatus == "NO_HISTORY")
  }

  test("replaced timer callback cannot remove newer state") {
    val processor = new CustomerAmountStatisticsProcessor(Duration.ofHours(1))
    val tester = newTester(processor)
    tester.test(key("a"), List(input("a", 0L, "12:00", "100")))
    val oldTimer = state(tester, "a").expiryTimerMs
    tester.test(key("a"), List(input("a", 1L, "12:10", "200")))
    assert(state(tester, "a").expiryTimerMs > oldTimer)
    assert(
      processor
        .handleExpiredTimer(
          key("a"),
          new TimerValues {
            override def getCurrentProcessingTimeInMs(): Long = 0L
            override def getCurrentWatermarkInMs(): Long = oldTimer
          },
          new ExpiredTimerInfo {
            override def getExpiryTimeInMs(): Long = oldTimer
          }
        )
        .isEmpty
    )
    assertState(tester, "a", 2L, 150.0, 5000.0)
  }

  test("only negligible negative variance is clamped; invalid numeric state fails") {
    assert(CustomerAmountStatisticsProcessor.checkedVariance(-1e-13) == 0.0)
    intercept[IllegalStateException] {
      CustomerAmountStatisticsProcessor.checkedVariance(-1e-6)
    }
    intercept[IllegalArgumentException] {
      CustomerAmountStatisticsProcessor.checkedVariance(Double.NaN)
    }
    intercept[IllegalArgumentException] {
      CustomerAmountStatisticsProcessor.finite(Double.PositiveInfinity, "test")
    }
    intercept[IllegalArgumentException] {
      TransactionStatisticalFeatures(
        input("a", 0L, "12:00", "100"),
        2L,
        Some(Double.NaN),
        Some(1.0),
        None,
        "READY"
      )
    }
  }

  test("alternating currencies isolate prior statistics, M2 and observed offsets") {
    val tester = newTester()
    val usd = key("a")
    val eur = key("a", "EUR")
    tester.test(usd, List(input("a", 0L, "12:00", "100")))
    val firstEur = tester.test(eur, List(input("a", 1L, "12:01", "1000", "EUR"))).head
    assert(firstEur.priorAmountObservationCount == 0L)
    assert(firstEur.priorAmountMean.isEmpty)
    assert(firstEur.statisticalFeatureStatus == "NO_HISTORY")
    val secondUsd = tester.test(usd, List(input("a", 2L, "12:02", "200"))).head
    assert(secondUsd.priorAmountObservationCount == 1L)
    assert(secondUsd.priorAmountMean.contains(100.0))
    assert(secondUsd.priorAmountStddev.isEmpty)
    assert(secondUsd.amountZscore.isEmpty)
    val secondEur = tester.test(eur, List(input("a", 3L, "12:03", "1200", "EUR"))).head
    assert(secondEur.priorAmountObservationCount == 1L)
    assert(secondEur.priorAmountMean.contains(1000.0))
    val thirdUsd = tester.test(usd, List(input("a", 4L, "12:04", "300"))).head
    assert(thirdUsd.priorAmountObservationCount == 2L)
    assertClose(thirdUsd.priorAmountMean.get, 150.0)
    assertClose(thirdUsd.priorAmountStddev.get, math.sqrt(5000.0))
    assertClose(thirdUsd.amountZscore.get, 150.0 / math.sqrt(5000.0))
    assert(thirdUsd.statisticalFeatureStatus == "READY")
    assertState(tester, "a", 3L, 200.0, 20000.0)
    val eurState = state(tester, "a", "EUR")
    assert(eurState.count == 2L)
    assertClose(eurState.mean, 1100.0)
    assertClose(eurState.m2, 20000.0)
    assert(state(tester, "a").lastObservedKafkaOffset == 4L)
    assert(eurState.lastObservedKafkaOffset == 3L)
    assert(eurState.observedKafkaPartition == state(tester, "a").observedKafkaPartition)
  }

  test("currency-specific expiry and reactivation leave another currency active") {
    val tester = newTester()
    tester.test(key("a"), List(input("a", 0L, "12:00", "100")))
    tester.test(key("a", "EUR"), List(input("a", 1L, "12:30", "1000", "EUR")))
    val usdTimer = state(tester, "a").expiryTimerMs
    val eurBefore = state(tester, "a", "EUR")
    assert(usdTimer < eurBefore.expiryTimerMs)
    assert(tester.setWatermark(usdTimer).isEmpty)
    assert(
      tester
        .peekValueState[CustomerAmountStatistics](
          CustomerAmountStatisticsProcessor.StateName,
          key("a")
        )
        .isEmpty
    )
    assert(state(tester, "a", "EUR") == eurBefore)
    val usdAgain = tester.test(key("a"), List(input("a", 2L, "13:01", "200"))).head
    assert(usdAgain.priorAmountObservationCount == 0L)
    assert(usdAgain.statisticalFeatureStatus == "NO_HISTORY")
    assert(state(tester, "a", "EUR") == eurBefore)
    val eurAgain = tester.test(key("a", "EUR"), List(input("a", 3L, "13:02", "1200", "EUR"))).head
    assert(eurAgain.priorAmountObservationCount == 1L)
    assert(eurAgain.priorAmountMean.contains(1000.0))
    assertState(tester, "a", 1L, 200.0, 0.0)
  }

  test("statistical key must match both customer and currency") {
    intercept[IllegalArgumentException] {
      newTester().test(key("a"), List(input("a", 0L, "12:00", "100", "EUR")))
    }
    intercept[IllegalArgumentException] {
      newTester().test(key("b"), List(input("a", 0L, "12:00", "100")))
    }
  }

  private def key(customer: String, currency: String = "USD"): CustomerCurrencyKey =
    CustomerCurrencyKey(customer, currency)

  private def newTester(
      processor: CustomerAmountStatisticsProcessor = new CustomerAmountStatisticsProcessor(
        Duration.ofHours(1)
      )
  ): TwsTester[CustomerCurrencyKey, TransactionCustomerFeatures, TransactionStatisticalFeatures] =
    new TwsTester[CustomerCurrencyKey, TransactionCustomerFeatures, TransactionStatisticalFeatures](
      processor = processor,
      timeMode = TimeMode.EventTime(),
      outputMode = OutputMode.Update(),
      eventTimeExtractor = Some(_.eventTime.getTime)
    )

  private def state(
      tester: TwsTester[
        CustomerCurrencyKey,
        TransactionCustomerFeatures,
        TransactionStatisticalFeatures
      ],
      customer: String,
      currency: String = "USD"
  ): CustomerAmountStatistics =
    tester
      .peekValueState[CustomerAmountStatistics](
        CustomerAmountStatisticsProcessor.StateName,
        key(customer, currency)
      )
      .get

  private def assertState(
      tester: TwsTester[
        CustomerCurrencyKey,
        TransactionCustomerFeatures,
        TransactionStatisticalFeatures
      ],
      key: String,
      count: Long,
      mean: Double,
      m2: Double
  ): Unit = {
    val current = state(tester, key)
    assert(current.count == count)
    assertClose(current.mean, mean)
    assertClose(current.m2, m2)
  }

  private def assertClose(actual: Double, expected: Double): Unit =
    assert(math.abs(actual - expected) < 1e-8)

  private def input(
      customer: String,
      offset: Long,
      hhmm: String,
      amount: String,
      currency: String = "USD"
  ): TransactionCustomerFeatures = {
    val time = Timestamp.from(instant(s"2030-01-01T${hhmm}:00Z"))
    TransactionCustomerFeatures(
      s"event-$customer-$offset",
      s"transaction-$customer-$offset",
      customer,
      "merchant",
      time,
      time,
      new java.math.BigDecimal(amount),
      currency,
      "US",
      "device",
      "192.0.2.1",
      "CARD_PAYMENT",
      1,
      customer,
      "transactions.raw",
      0,
      offset,
      time,
      0L,
      "0.0000"
    )
  }

  private def instant(value: String): Instant = Instant.parse(value)

  private def epochMicros(hhmm: String): Long =
    instant(s"2030-01-01T${hhmm}:00Z").getEpochSecond * 1000000L
}
