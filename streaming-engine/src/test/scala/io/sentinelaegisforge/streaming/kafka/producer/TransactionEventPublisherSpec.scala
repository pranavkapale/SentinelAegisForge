package io.sentinelaegisforge.streaming.kafka.producer

import java.time.Instant

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.kafka.producer.TransactionPublishError.DeliveryFailed
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroSchema
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class TransactionEventPublisherSpec extends AnyFunSuite {
  test("a bounded publishing run accounts for acknowledgements and closes the producer") {
    val producer = mockProducer()
    val events = generatedEvents(3)
    val report = new TransactionEventPublisher(producer, recordFactory)
      .publish(events)
      .fold(error => fail(error.message), identity)

    assert(report.requested == 3)
    assert(report.acknowledgedCount == 3)
    assert(report.failedCount == 0)
    assert(report.acknowledged.forall(_.topic == TransactionProducerConfig.DefaultTopic))
    assert(producer.history().size() == 3)
    assert(producer.closed())
  }

  test("a Kafka send failure is surfaced and the producer is still closed") {
    val producer = mockProducer()
    producer.sendException = new RuntimeException("simulated send failure")

    new TransactionEventPublisher(producer, recordFactory).publish(generatedEvents(1)) match {
      case Left(DeliveryFailed(report)) =>
        assert(report.requested == 1)
        assert(report.acknowledgedCount == 0)
        assert(report.failedCount == 1)
        assert(report.failures.head.details == "simulated send failure")
      case other => fail(s"Expected a surfaced delivery failure, received: $other")
    }
    assert(producer.closed())
  }

  private def generatedEvents(count: Int): Vector[TransactionEvent] =
    new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(42L, Instant.parse("2030-01-01T00:00:00Z"))
    ).generateValidCandidates(count).map { candidate =>
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

  private def mockProducer(): MockProducer[String, GenericRecord] =
    new MockProducer[String, GenericRecord](
      true,
      null,
      new StringSerializer(),
      new Serializer[GenericRecord] {
        override def serialize(topic: String, data: GenericRecord): Array[Byte] =
          Array.emptyByteArray
      }
    )
}
