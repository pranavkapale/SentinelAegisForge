package io.sentinelaegisforge.streaming.serialization.avro

import scala.util.control.NonFatal

import org.apache.avro.Schema

import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError._

object TransactionEventAvroSchema {
  val ResourcePath: String = "contracts/events/transaction-event-v1.avsc"

  lazy val load: Either[TransactionEventAvroError, Schema] = {
    val resource = Option(getClass.getClassLoader.getResourceAsStream(ResourcePath))
    resource match {
      case None        => Left(SchemaResourceMissing(ResourcePath))
      case Some(input) =>
        try {
          Right(new Schema.Parser().parse(input))
        } catch {
          case NonFatal(error) => Left(SchemaParsingFailed(error.getMessage))
        } finally {
          input.close()
        }
    }
  }
}
