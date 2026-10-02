package io.sentinelaegisforge.streaming.processing.rollingfeatures

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.streaming.{
  ExpiredTimerInfo,
  OutputMode,
  TimeMode,
  TimerValues,
  TwsTester
}
import org.scalatest.funsuite.AnyFunSuite

final class RollingFeatureProcessorSpec extends AnyFunSuite {
  test("first event excludes itself and sequential windows include their lower bounds") {
    val tester = newTester()
    val first = tester.test("a", List(input("a", "a0", "2030-01-01T12:00:00Z", "100")))
    assertFeatures(first.head, 0L, "0.0000")
    assert(history(tester, "a").keySet == Set("a0"))

    val second = tester.test("a", List(input("a", "a3", "2030-01-01T12:03:00Z", "200")))
    assertFeatures(second.head, 1L, "100.0000")

    val third = tester.test("a", List(input("a", "a7", "2030-01-01T12:07:00Z", "300")))
    assertFeatures(third.head, 1L, "300.0000")

    val boundary = tester.test("a", List(input("a", "a10", "2030-01-01T12:10:00Z", "1")))
    assertFeatures(boundary.head, 1L, "600.0000")
  }

  test("equal-time peers cannot count one another regardless of iterator order or batch") {
    val a = input("a", "event-a", "2030-01-01T12:00:00Z", "10")
    val b = input("a", "event-b", "2030-01-01T12:00:00Z", "20")
    val forward = newTester().test("a", List(a, b))
    val reverse = newTester().test("a", List(b, a))
    assert(forward == reverse)
    forward.foreach(assertFeatures(_, 0L, "0.0000"))

    val separateBatches = newTester()
    assertFeatures(separateBatches.test("a", List(a)).head, 0L, "0.0000")
    assertFeatures(separateBatches.test("a", List(b)).head, 0L, "0.0000")
  }

  test("out-of-order event excludes stored future events and does not rewrite old output") {
    val tester = newTester()
    tester.test("a", List(input("a", "at0", "2030-01-01T12:00:00Z", "100")))
    tester.test("a", List(input("a", "at5", "2030-01-01T12:05:00Z", "200")))
    val atTen = tester.test("a", List(input("a", "at10", "2030-01-01T12:10:00Z", "300"))).head
    assertFeatures(atTen, 1L, "300.0000")

    val late = tester.test("a", List(input("a", "at8", "2030-01-01T12:08:00Z", "40"))).head
    assertFeatures(late, 1L, "300.0000")
    assertFeatures(atTen, 1L, "300.0000")
  }

  test("microsecond comparisons and cleanup timers never discard history early") {
    val tester = newTester()
    tester.test("a", List(input("a", "micro", "2030-01-01T12:00:00.000001Z", "1.2500")))
    val timer = tester
      .peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a")
      .get
    assert(timer == instant("2030-01-01T12:10:00.001Z").toEpochMilli)
    val boundary = tester
      .test(
        "a",
        List(input("a", "boundary", "2030-01-01T12:10:00.000001Z", "2"))
      )
      .head
    assertFeatures(boundary, 0L, "1.2500")
    tester.setWatermark(instant("2030-01-01T12:10:00Z").toEpochMilli)
    assert(history(tester, "a").contains("micro"))
    tester.setWatermark(instant("2030-01-01T12:10:00.001Z").toEpochMilli)
    assert(!history(tester, "a").contains("micro"))
    assert(history(tester, "a").contains("boundary"))
    assert(
      tester.peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a") ==
        Some(instant("2030-01-01T12:20:00.001Z").toEpochMilli)
    )
    tester.setWatermark(instant("2030-01-01T12:20:00.001Z").toEpochMilli)
    assert(history(tester, "a").isEmpty)
    assert(
      tester.peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a").isEmpty
    )
  }

  test("earlier out-of-order expiry replaces one timer and stale callback is harmless") {
    val processor = new RollingFeatureProcessor
    val tester = newTester(processor)
    tester.test("a", List(input("a", "late", "2030-01-01T12:10:00Z", "1")))
    val oldTimer =
      tester.peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a").get
    tester.test("a", List(input("a", "early", "2030-01-01T12:05:00Z", "1")))
    val newTimer =
      tester.peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a").get
    assert(newTimer < oldTimer)
    val stale = processor
      .handleExpiredTimer(
        "a",
        fixedTimerValues(oldTimer),
        fixedExpiredTimer(oldTimer)
      )
      .toVector
    assert(stale.isEmpty)
    assert(history(tester, "a").keySet == Set("late", "early"))
    tester.setWatermark(newTimer)
    assert(history(tester, "a").keySet == Set("late"))
  }

  test("one timer callback clears every entry due after a watermark jump") {
    val tester = newTester()
    tester.test(
      "a",
      List(
        input("a", "at0", "2030-01-01T12:00:00Z", "1"),
        input("a", "at1", "2030-01-01T12:01:00Z", "1"),
        input("a", "at8", "2030-01-01T12:08:00Z", "1")
      )
    )
    assert(history(tester, "a").size == 3)
    tester.setWatermark(instant("2030-01-01T12:20:00Z").toEpochMilli)
    assert(history(tester, "a").isEmpty)
    assert(
      tester.peekValueState[Long](RollingFeatureProcessor.NextCleanupTimerStateName, "a").isEmpty
    )
  }

  test("customers remain independent and monetary overflow fails explicitly") {
    val tester = newTester()
    tester.test("a", List(input("a", "a0", "2030-01-01T12:00:00Z", "5")))
    val b = tester.test("b", List(input("b", "b0", "2030-01-01T12:01:00Z", "7")))
    assertFeatures(b.head, 0L, "0.0000")
    assert(history(tester, "a").keySet == Set("a0"))
    assert(history(tester, "b").keySet == Set("b0"))

    assert(
      RollingFeatureProcessor.checkedSum(new java.math.BigDecimal("1.2345")) ==
        new java.math.BigDecimal("1.2345")
    )
    intercept[ArithmeticException] {
      val maximum = new java.math.BigDecimal(("9" * 34) + ".9999")
      RollingFeatureProcessor.checkedSum(maximum.add(new java.math.BigDecimal("0.0001")))
    }
    intercept[ArithmeticException] {
      RollingFeatureProcessor.checkedSum(new java.math.BigDecimal("1.00001"))
    }
  }

  test("velocity includes every currency while amount membership matches the current currency") {
    val tester = newTester()
    tester.test("a", List(input("a", "usd0", "2030-01-01T12:00:00Z", "100")))
    val eur = tester.test("a", List(input("a", "eur1", "2030-01-01T12:01:00Z", "200", "EUR"))).head
    assertFeatures(eur, 1L, "0.0000")
    val usd = tester.test("a", List(input("a", "usd2", "2030-01-01T12:02:00Z", "300"))).head
    assertFeatures(usd, 2L, "100.0000")
    val nextEur =
      tester.test("a", List(input("a", "eur3", "2030-01-01T12:03:00Z", "400", "EUR"))).head
    assertFeatures(nextEur, 3L, "200.0000")
    assert(history(tester, "a")("eur1").currency == "EUR")
  }

  test("exact inclusive monetary lower bound also requires currency equality") {
    val tester = newTester()
    tester.test(
      "a",
      List(
        input("a", "usd0", "2030-01-01T12:00:00Z", "100"),
        input("a", "eur0", "2030-01-01T12:00:00Z", "500", "EUR")
      )
    )
    val boundary = tester.test("a", List(input("a", "usd10", "2030-01-01T12:10:00Z", "1"))).head
    assertFeatures(boundary, 0L, "100.0000")
  }

  test("out-of-order monetary history excludes other currencies and future-event-time state") {
    val tester = newTester()
    tester.test("a", List(input("a", "usd0", "2030-01-01T12:00:00Z", "100")))
    tester.test("a", List(input("a", "eur5", "2030-01-01T12:05:00Z", "200", "EUR")))
    val atTen = tester.test("a", List(input("a", "usd10", "2030-01-01T12:10:00Z", "300"))).head
    assertFeatures(atTen, 1L, "100.0000")
    val late = tester.test("a", List(input("a", "usd8", "2030-01-01T12:08:00Z", "40"))).head
    assertFeatures(late, 1L, "100.0000")
    assertFeatures(atTen, 1L, "100.0000")
  }

  test("mixed-currency same-time peers read prior history without influencing each other") {
    val tester = newTester()
    tester.test(
      "a",
      List(
        input("a", "usd0", "2030-01-01T12:00:00Z", "100"),
        input("a", "eur0", "2030-01-01T12:00:00Z", "500", "EUR")
      )
    )
    val peers = tester
      .test(
        "a",
        List(
          input("a", "eur1", "2030-01-01T12:01:00Z", "200", "EUR"),
          input("a", "usd1", "2030-01-01T12:01:00Z", "300")
        )
      )
      .map(row => row.currency -> row)
      .toMap
    assertFeatures(peers("USD"), 2L, "100.0000")
    assertFeatures(peers("EUR"), 2L, "500.0000")
    val laterPeer = tester.test("a", List(input("a", "usd1b", "2030-01-01T12:01:00Z", "400"))).head
    assertFeatures(laterPeer, 2L, "100.0000")
  }

  private def newTester(
      processor: RollingFeatureProcessor = new RollingFeatureProcessor
  ): TwsTester[String, RollingFeatureInput, TransactionCustomerFeatures] =
    new TwsTester[String, RollingFeatureInput, TransactionCustomerFeatures](
      processor = processor,
      timeMode = TimeMode.EventTime(),
      outputMode = OutputMode.Update(),
      eventTimeExtractor = Some(_.eventTime.getTime)
    )

  private def history(
      tester: TwsTester[String, RollingFeatureInput, TransactionCustomerFeatures],
      key: String
  ) =
    tester.peekMapState[String, RollingCustomerEvent](
      RollingFeatureProcessor.RollingEventsStateName,
      key
    )

  private def assertFeatures(row: TransactionCustomerFeatures, count: Long, sum: String): Unit = {
    assert(row.priorTransactionCount5m == count)
    assert(
      new java.math.BigDecimal(row.priorAmountSum10m).compareTo(new java.math.BigDecimal(sum)) == 0
    )
  }

  private def input(
      customer: String,
      eventId: String,
      time: String,
      amount: String,
      currency: String = "USD"
  ): RollingFeatureInput = {
    val eventTime = Timestamp.from(instant(time))
    RollingFeatureInput(
      eventId,
      s"transaction-$eventId",
      customer,
      "merchant-a",
      eventTime,
      eventTime,
      new java.math.BigDecimal(amount),
      currency,
      "US",
      "device-a",
      "192.0.2.1",
      "CARD_PAYMENT",
      1,
      customer,
      "transactions.raw",
      0,
      0L,
      eventTime
    )
  }

  private def instant(value: String): Instant = Instant.parse(value)

  private def fixedTimerValues(watermarkMs: Long): TimerValues = new TimerValues {
    override def getCurrentProcessingTimeInMs(): Long = 0L
    override def getCurrentWatermarkInMs(): Long = watermarkMs
  }

  private def fixedExpiredTimer(expiryMs: Long): ExpiredTimerInfo = new ExpiredTimerInfo {
    override def getExpiryTimeInMs(): Long = expiryMs
  }
}
