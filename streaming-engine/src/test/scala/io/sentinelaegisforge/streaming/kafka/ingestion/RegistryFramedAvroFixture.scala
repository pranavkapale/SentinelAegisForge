package io.sentinelaegisforge.streaming.kafka.ingestion

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.avro.generic.GenericRecord

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.sentinelaegisforge.streaming.domain.transaction.TransactionEvent
import io.sentinelaegisforge.streaming.serialization.avro.{
  TransactionEventAvroMapper,
  TransactionEventAvroSchema
}

private[ingestion] final class RegistryFramedAvroFixture extends AutoCloseable {
  private val scope = s"phase5-${UUID.randomUUID()}"
  val registryUrl: String = s"mock://$scope"

  private val serializer = new KafkaAvroSerializer()
  serializer.configure(
    Map[String, AnyRef](
      "schema.registry.url" -> registryUrl,
      "auto.register.schemas" -> Boolean.box(true)
    ).asJava,
    false
  )

  def serialize(event: TransactionEvent): Array[Byte] =
    serializeRecord(
      TransactionEventAvroMapper
        .toRecord(event, schema)
        .fold(error => throw new IllegalStateException(error.message), identity)
    )

  def serializeRecord(record: GenericRecord): Array[Byte] =
    serializer.serialize(TransactionIngestionConfig.DefaultTopic, record)

  def record(event: TransactionEvent): GenericRecord =
    TransactionEventAvroMapper
      .toRecord(event, schema)
      .fold(error => throw new IllegalStateException(error.message), identity)

  override def close(): Unit = {
    serializer.close()
    MockSchemaRegistry.dropScope(scope)
  }

  private val schema = TransactionEventAvroSchema.load.fold(
    error => throw new IllegalStateException(error.message),
    identity
  )
}
