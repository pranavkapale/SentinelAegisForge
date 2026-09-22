package io.sentinelaegisforge.streaming.kafka.producer

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroSchema
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class TransactionProducerRecordFactorySpec extends AnyFunSuite {
  test(
    "a producer record uses transactions.raw, customerId, the canonical schema, and no partition"
  ) {
    val event = generatedEvent(seed = 42L)
    val record = recordFactory.create(event).fold(error => fail(error.message), identity)

    assert(record.topic() == TransactionProducerConfig.DefaultTopic)
    assert(record.key() == event.customerId)
    assert(record.value().getSchema == canonicalSchema)
    assert(record.partition() == null)
  }

  test("the deterministic generated pipeline validates, maps, and constructs a Kafka record") {
    val first = generatedEvent(seed = 99L)
    val second = generatedEvent(seed = 99L)
    val firstRecord = recordFactory.create(first).fold(error => fail(error.message), identity)
    val secondRecord = recordFactory.create(second).fold(error => fail(error.message), identity)

    assert(first == second)
    assert(firstRecord.key() == secondRecord.key())
    assert(firstRecord.value() == secondRecord.value())
  }

  private def generatedEvent(seed: Long): TransactionEvent = {
    val candidate = new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(seed, Instant.parse("2030-01-01T00:00:00Z"))
    ).generateValidCandidates(1).head
    TransactionEventValidator
      .validate(candidate)
      .fold(
        errors => fail(s"Generated candidate failed validation: $errors"),
        identity
      )
  }

  private lazy val canonicalSchema = TransactionEventAvroSchema.load.fold(
    error => fail(error.message),
    identity
  )
  private lazy val recordFactory =
    new TransactionProducerRecordFactory(TransactionProducerConfig.DefaultTopic, canonicalSchema)
}
