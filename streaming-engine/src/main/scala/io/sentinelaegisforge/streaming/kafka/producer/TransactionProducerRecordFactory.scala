package io.sentinelaegisforge.streaming.kafka.producer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord

import io.sentinelaegisforge.streaming.domain.transaction.TransactionEvent
import io.sentinelaegisforge.streaming.serialization.avro.{
  TransactionEventAvroError,
  TransactionEventAvroMapper
}

final class TransactionProducerRecordFactory(topic: String, schema: Schema) {
  require(topic.trim.nonEmpty, "topic must not be blank")

  def create(
      event: TransactionEvent
  ): Either[TransactionEventAvroError, ProducerRecord[String, GenericRecord]] =
    TransactionEventAvroMapper
      .toRecord(event, schema)
      .map(record => new ProducerRecord[String, GenericRecord](topic, event.customerId, record))
}
