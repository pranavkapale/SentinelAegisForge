package io.sentinelaegisforge.streaming.simulation

import java.nio.ByteBuffer
import java.util.UUID

import scala.util.Random

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEventCandidate,
  TransactionType
}

final class DeterministicTransactionGenerator(config: TransactionGeneratorConfig) {
  private val currencies = Vector("USD", "EUR", "GBP")
  private val countries = Vector("US", "GB", "DE")
  private val documentationIpPrefixes = Vector("192.0.2", "198.51.100", "203.0.113")

  /** Generates the same candidate sequence for every invocation with this configuration. */
  def generateValidCandidates(count: Int): Vector[TransactionEventCandidate] = {
    require(count >= 0, "count must not be negative")
    val random = new Random(config.seed)

    Vector.tabulate(count) { index =>
      val eventTime = config.baseEventTime.plus(config.eventSpacing.multipliedBy(index.toLong))
      val ingestionTime = eventTime.plus(config.ingestionDelay)
      val cents = 100 + random.nextInt(999900)
      val amount = f"${cents / 100}%d.${cents % 100}%02d"
      val ipPrefix = documentationIpPrefixes(random.nextInt(documentationIpPrefixes.size))

      TransactionEventCandidate(
        eventId = Some(nextUuid(random).toString),
        transactionId = Some(s"txn-${nextUuid(random)}"),
        customerId = Some(f"customer-${random.nextInt(10000)}%04d"),
        merchantId = Some(f"merchant-${random.nextInt(1000)}%03d"),
        eventTime = Some(eventTime.toString),
        ingestionTime = Some(ingestionTime.toString),
        amount = Some(amount),
        currency = Some(currencies(random.nextInt(currencies.size))),
        country = Some(countries(random.nextInt(countries.size))),
        deviceId = Some(f"device-${random.nextInt(100000)}%05d"),
        ipAddress = Some(s"$ipPrefix.${1 + random.nextInt(254)}"),
        transactionType = Some(TransactionType.CardPayment.externalName),
        schemaVersion = Some("1")
      )
    }
  }

  private def nextUuid(random: Random): UUID = {
    val bytes = Array.fill[Byte](16)(random.nextInt(256).toByte)
    bytes(6) = ((bytes(6) & 0x0f) | 0x40).toByte
    bytes(8) = ((bytes(8) & 0x3f) | 0x80).toByte
    val buffer = ByteBuffer.wrap(bytes)
    new UUID(buffer.getLong, buffer.getLong)
  }
}
