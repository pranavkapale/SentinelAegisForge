package io.sentinelaegisforge.streaming.kafka.ingestion

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionError._
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError.DomainValidationFailed
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class RegistryBackedTransactionDecoderSpec extends AnyFunSuite {
  test("registry-framed Avro decodes through the domain boundary and preserves metadata") {
    withFixture { fixture =>
      val event = generatedEvent()
      val source = sourceRecord(
        key = event.customerId.getBytes(StandardCharsets.UTF_8),
        value = fixture.serialize(event)
      )
      val decoded = decode(fixture, source)

      assert(decoded.eventId == event.eventId.toString)
      assert(decoded.customerId == event.customerId)
      assert(decoded.kafkaKey == event.customerId)
      assert(decoded.kafkaTopic == source.topic)
      assert(decoded.kafkaPartition == source.partition)
      assert(decoded.kafkaOffset == source.offset)
      assert(decoded.kafkaTimestamp == source.timestamp)
    }
  }

  test("a null Kafka key is rejected explicitly") {
    withFixture { fixture =>
      val event = generatedEvent()
      withDecoder(fixture) { decoder =>
        decoder.decode(sourceRecord(null, fixture.serialize(event))) match {
          case Left(NullKafkaKey(coordinates)) => assert(coordinates.offset == 17L)
          case other => fail(s"Expected a null-key error, received: $other")
        }
      }
    }
  }

  test("a malformed UTF-8 Kafka key is rejected explicitly") {
    withFixture { fixture =>
      val event = generatedEvent()
      val invalidUtf8 = Array(0xc3.toByte, 0x28.toByte)

      withDecoder(fixture) { decoder =>
        decoder.decode(sourceRecord(invalidUtf8, fixture.serialize(event))) match {
          case Left(InvalidUtf8KafkaKey(coordinates)) => assert(coordinates.partition == 2)
          case other => fail(s"Expected an invalid-UTF-8 error, received: $other")
        }
      }
    }
  }

  test("a Kafka key that differs from customer_id is rejected") {
    withFixture { fixture =>
      val event = generatedEvent()
      withDecoder(fixture) { decoder =>
        decoder.decode(
          sourceRecord(
            "different-customer".getBytes(StandardCharsets.UTF_8),
            fixture.serialize(event)
          )
        ) match {
          case Left(CustomerKeyMismatch(_, key, customerId)) =>
            assert(key == "different-customer")
            assert(customerId == event.customerId)
          case other => fail(s"Expected a customer-key mismatch, received: $other")
        }
      }
    }
  }

  test("malformed registry framing returns a controlled decode error") {
    withFixture { fixture =>
      withDecoder(fixture) { decoder =>
        decoder.decode(
          sourceRecord("customer-1".getBytes(StandardCharsets.UTF_8), Array[Byte](1, 2, 3))
        ) match {
          case Left(_: RegistryAvroDecodeFailed) => assert(true)
          case other => fail(s"Expected a registry decode error, received: $other")
        }
      }
    }
  }

  test("a semantically invalid decoded record cannot bypass domain validation") {
    withFixture { fixture =>
      val event = generatedEvent()
      val invalidRecord = fixture.record(event)
      invalidRecord.put("customer_id", " ")
      val value = fixture.serializeRecord(invalidRecord)

      withDecoder(fixture) { decoder =>
        decoder.decode(
          sourceRecord(event.customerId.getBytes(StandardCharsets.UTF_8), value)
        ) match {
          case Left(DomainDecodingFailed(_, DomainValidationFailed(errors))) =>
            assert(errors.nonEmpty)
          case other => fail(s"Expected a domain validation error, received: $other")
        }
      }
    }
  }

  private def decode(
      fixture: RegistryFramedAvroFixture,
      source: KafkaSourceRecord
  ): ValidatedTransactionRecord = {
    val decoder = configuredDecoder(fixture)
    try decoder.decode(source).fold(error => fail(error.message), identity)
    finally decoder.close()
  }

  private def configuredDecoder(
      fixture: RegistryFramedAvroFixture
  ): RegistryBackedTransactionDecoder =
    RegistryBackedTransactionDecoder
      .configured(fixture.registryUrl)
      .fold(error => fail(error.message), identity)

  private def withDecoder(
      fixture: RegistryFramedAvroFixture
  )(test: RegistryBackedTransactionDecoder => Unit): Unit = {
    val decoder = configuredDecoder(fixture)
    try test(decoder)
    finally decoder.close()
  }

  private def sourceRecord(key: Array[Byte], value: Array[Byte]): KafkaSourceRecord =
    KafkaSourceRecord(
      key = key,
      value = value,
      topic = TransactionIngestionConfig.DefaultTopic,
      partition = 2,
      offset = 17L,
      timestamp = Timestamp.from(Instant.parse("2030-01-01T00:00:05Z"))
    )

  private def generatedEvent(): TransactionEvent = {
    val candidate = new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(42L, Instant.parse("2030-01-01T00:00:00Z"))
    ).generateValidCandidates(1).head
    TransactionEventValidator
      .validate(candidate)
      .fold(errors => fail(s"Generated candidate failed validation: $errors"), identity)
  }

  private def withFixture(test: RegistryFramedAvroFixture => Unit): Unit = {
    val fixture = new RegistryFramedAvroFixture()
    try test(fixture)
    finally fixture.close()
  }
}
