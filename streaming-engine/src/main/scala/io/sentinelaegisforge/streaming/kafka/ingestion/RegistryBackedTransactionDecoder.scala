package io.sentinelaegisforge.streaming.kafka.ingestion

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.avro.generic.GenericRecord

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionError._
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroMapper

final class RegistryBackedTransactionDecoder private (
    deserializer: KafkaAvroDeserializer
) extends AutoCloseable {
  def decode(
      source: KafkaSourceRecord
  ): Either[TransactionIngestionError, ValidatedTransactionRecord] =
    for {
      key <- decodeKey(source)
      value <- Option(source.value).toRight(NullKafkaValue(source.coordinates))
      decoded <- deserialize(source, value)
      record <- decoded match {
        case generic: GenericRecord => Right(generic)
        case other                  =>
          Left(
            DecodedValueWasNotGenericRecord(
              source.coordinates,
              Option(other).map(_.getClass.getName).getOrElse("null")
            )
          )
      }
      event <- TransactionEventAvroMapper
        .fromRecord(record)
        .left
        .map(DomainDecodingFailed(source.coordinates, _))
      _ <- Either.cond(
        key == event.customerId,
        (),
        CustomerKeyMismatch(source.coordinates, key, event.customerId)
      )
    } yield ValidatedTransactionRecord.from(event, key, source)

  override def close(): Unit = deserializer.close()

  private def decodeKey(source: KafkaSourceRecord): Either[TransactionIngestionError, String] =
    Option(source.key).toRight(NullKafkaKey(source.coordinates)).flatMap { keyBytes =>
      try {
        val decoder = StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
        Right(decoder.decode(ByteBuffer.wrap(keyBytes)).toString)
      } catch {
        case NonFatal(_) => Left(InvalidUtf8KafkaKey(source.coordinates))
      }
    }

  private def deserialize(
      source: KafkaSourceRecord,
      value: Array[Byte]
  ): Either[TransactionIngestionError, AnyRef] =
    try Right(deserializer.deserialize(source.topic, value))
    catch {
      case NonFatal(error) =>
        Left(RegistryAvroDecodeFailed(source.coordinates, errorMessage(error)))
    }

  private def errorMessage(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
}

object RegistryBackedTransactionDecoder {
  def configured(
      schemaRegistryUrl: String
  ): Either[TransactionIngestionError, RegistryBackedTransactionDecoder] = {
    val deserializer = new KafkaAvroDeserializer()
    val configuration = Map[String, AnyRef](
      "schema.registry.url" -> schemaRegistryUrl,
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> Boolean.box(false)
    ).asJava

    try {
      deserializer.configure(configuration, false)
      Right(new RegistryBackedTransactionDecoder(deserializer))
    } catch {
      case NonFatal(error) =>
        deserializer.close()
        Left(
          DeserializerConfigurationFailed(
            Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
          )
        )
    }
  }
}
