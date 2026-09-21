package io.sentinelaegisforge.streaming.serialization.avro

import java.io.ByteArrayOutputStream

import scala.util.control.NonFatal

import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import io.sentinelaegisforge.streaming.domain.transaction.TransactionEvent
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError._

object TransactionEventAvroCodec {
  def encode(event: TransactionEvent): Either[TransactionEventAvroError, Array[Byte]] =
    for {
      schema <- TransactionEventAvroSchema.load
      record <- TransactionEventAvroMapper.toRecord(event, schema)
      bytes <- encodeRecord(record, schema)
    } yield bytes

  def decode(bytes: Array[Byte]): Either[TransactionEventAvroError, TransactionEvent] =
    if (bytes == null) {
      Left(BinaryDecodingFailed("input bytes were null"))
    } else {
      for {
        schema <- TransactionEventAvroSchema.load
        record <-
          try {
            val reader = new GenericDatumReader[org.apache.avro.generic.GenericRecord](schema)
            val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
            Right(reader.read(null, decoder))
          } catch {
            case NonFatal(error) => Left(BinaryDecodingFailed(error.getMessage))
          }
        event <- TransactionEventAvroMapper.fromRecord(record)
      } yield event
    }

  private def encodeRecord(
      record: org.apache.avro.generic.GenericRecord,
      schema: org.apache.avro.Schema
  ): Either[TransactionEventAvroError, Array[Byte]] =
    try {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, null)
      new GenericDatumWriter[org.apache.avro.generic.GenericRecord](schema).write(record, encoder)
      encoder.flush()
      Right(output.toByteArray)
    } catch {
      case NonFatal(error) => Left(BinaryEncodingFailed(error.getMessage))
    }
}
