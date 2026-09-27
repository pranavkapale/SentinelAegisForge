package io.sentinelaegisforge.streaming.processing.customerstate

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

final class CustomerActivityProcessorSpec extends AnyFunSuite {
  private val Inactivity = Duration.ofHours(1L)

  test("initial and repeated customer events update one exact aggregate state") {
    val tester = newTester()
    val first = tester.test(
      "customer-a",
      List(
        input("customer-a", "2030-01-01T12:00:00Z", "10.0000"),
        input("customer-a", "2030-01-01T12:05:00Z", "20.2500")
      )
    )

    assert(first.size == 1)
    assert(first.head.lifecycleType == CustomerActivityLifecycle.Updated.externalName)
    assert(first.head.batchEventCount == 2L)
    assert(first.head.batchAmountTotal.compareTo(decimal("30.2500")) == 0)
    assert(first.head.stateEventCount == 2L)
    assert(first.head.stateAmountTotal.compareTo(decimal("30.2500")) == 0)
    assert(first.head.stateFirstEventTime == timestamp("2030-01-01T12:00:00Z"))
    assert(first.head.stateLatestEventTime == timestamp("2030-01-01T12:05:00Z"))
    assert(first.head.stateExpiryTime == timestamp("2030-01-01T13:05:00Z"))

    val repeated = tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:10:00Z", "4.7500"))
    )

    assert(repeated.head.batchEventCount == 1L)
    assert(repeated.head.stateEventCount == 3L)
    assert(repeated.head.stateAmountTotal.compareTo(decimal("35.0000")) == 0)
    assert(repeated.head.stateExpiryTime == timestamp("2030-01-01T13:10:00Z"))
  }

  test("out-of-order input updates aggregates without moving latest time or timer backward") {
    val tester = newTester()
    tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:10:00Z", "10.0000"))
    )

    val output = tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:02:00Z", "2.5000"))
    )
    val state = tester
      .peekValueState[CustomerActivityState](CustomerActivityProcessor.StateName, "customer-a")
      .getOrElse(fail("Expected customer state"))

    assert(output.head.stateEventCount == 2L)
    assert(output.head.stateAmountTotal.compareTo(decimal("12.5000")) == 0)
    assert(state.firstEventTime == timestamp("2030-01-01T12:02:00Z"))
    assert(state.latestEventTime == timestamp("2030-01-01T12:10:00Z"))
    assert(state.expiryTimerTimestamp == instant("2030-01-01T13:10:00Z").toEpochMilli)
  }

  test("a stale timer callback cannot clear newer customer state") {
    val processor = new CustomerActivityProcessor(Inactivity)
    val tester = newTester(processor)
    tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:00:00Z", "10.0000"))
    )
    tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:10:00Z", "5.0000"))
    )

    val stale = processor
      .handleExpiredTimer(
        "customer-a",
        fixedTimerValues(instant("2030-01-01T13:05:00Z").toEpochMilli),
        fixedExpiredTimer(instant("2030-01-01T13:00:00Z").toEpochMilli)
      )
      .toVector

    assert(stale.isEmpty)
    assert(
      tester
        .peekValueState[CustomerActivityState](CustomerActivityProcessor.StateName, "customer-a")
        .exists(_.eventCount == 2L)
    )
  }

  test("event-time timer emits expiration, clears state, and a later event starts fresh") {
    val tester = newTester()
    tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:00:00Z", "10.0000"))
    )

    val expired = tester.setWatermark(instant("2030-01-01T13:00:00Z").toEpochMilli)

    assert(expired.size == 1)
    assert(expired.head.lifecycleType == CustomerActivityLifecycle.Expired.externalName)
    assert(expired.head.batchEventCount == 0L)
    assert(expired.head.stateEventCount == 1L)
    assert(
      tester
        .peekValueState[CustomerActivityState](CustomerActivityProcessor.StateName, "customer-a")
        .isEmpty
    )

    val reactivated = tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T13:05:00Z", "7.0000"))
    )
    assert(reactivated.head.stateEventCount == 1L)
    assert(reactivated.head.stateAmountTotal.compareTo(decimal("7.0000")) == 0)
  }

  test("customer keys maintain independent state") {
    val tester = newTester()
    tester.test(
      "customer-a",
      List(input("customer-a", "2030-01-01T12:00:00Z", "10.0000"))
    )
    tester.test(
      "customer-b",
      List(input("customer-b", "2030-01-01T12:01:00Z", "20.0000"))
    )

    val stateA = tester
      .peekValueState[CustomerActivityState](CustomerActivityProcessor.StateName, "customer-a")
      .get
    val stateB = tester
      .peekValueState[CustomerActivityState](CustomerActivityProcessor.StateName, "customer-b")
      .get
    assert(stateA.amountTotal.compareTo(decimal("10.0000")) == 0)
    assert(stateB.amountTotal.compareTo(decimal("20.0000")) == 0)
  }

  test("monetary state rejects values that require rounding or exceed decimal precision") {
    intercept[ArithmeticException] {
      CustomerActivityProcessor.checkedAmount(
        new java.math.BigDecimal("1.0000000000000000001")
      )
    }
    intercept[ArithmeticException] {
      CustomerActivityProcessor.checkedAmount(
        new java.math.BigDecimal("123456789012345678901.0000")
      )
    }
  }

  private def newTester(
      processor: CustomerActivityProcessor = new CustomerActivityProcessor(Inactivity)
  ): TwsTester[String, CustomerActivityInput, CustomerActivitySnapshot] =
    new TwsTester[String, CustomerActivityInput, CustomerActivitySnapshot](
      processor = processor,
      timeMode = TimeMode.EventTime(),
      outputMode = OutputMode.Update(),
      eventTimeExtractor = Some(_.event_time.getTime)
    )

  private def input(customerId: String, time: String, amount: String): CustomerActivityInput =
    CustomerActivityInput(customerId, timestamp(time), new java.math.BigDecimal(amount))

  private def decimal(value: String): java.math.BigDecimal =
    new java.math.BigDecimal(value).setScale(CustomerActivityProcessor.AmountScale)

  private def instant(value: String): Instant = Instant.parse(value)

  private def timestamp(value: String): Timestamp = Timestamp.from(instant(value))

  private def fixedTimerValues(watermarkMs: Long): TimerValues = new TimerValues {
    override def getCurrentProcessingTimeInMs(): Long = 0L
    override def getCurrentWatermarkInMs(): Long = watermarkMs
  }

  private def fixedExpiredTimer(expiryMs: Long): ExpiredTimerInfo = new ExpiredTimerInfo {
    override def getExpiryTimeInMs(): Long = expiryMs
  }
}
