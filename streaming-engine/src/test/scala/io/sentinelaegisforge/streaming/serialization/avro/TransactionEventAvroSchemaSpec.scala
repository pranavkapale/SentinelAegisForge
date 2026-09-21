package io.sentinelaegisforge.streaming.serialization.avro

import java.util.Arrays

import scala.jdk.CollectionConverters._

import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaCompatibility}
import org.scalatest.funsuite.AnyFunSuite

final class TransactionEventAvroSchemaSpec extends AnyFunSuite {
  test("the canonical v1 schema parses with its expected fields and logical types") {
    val schema = loadedSchema
    val expectedFields = Vector(
      "event_id",
      "transaction_id",
      "customer_id",
      "merchant_id",
      "event_time",
      "ingestion_time",
      "amount",
      "currency",
      "country",
      "device_id",
      "ip_address",
      "transaction_type",
      "schema_version"
    )

    assert(schema.getFields.asScala.map(_.name()).toVector == expectedFields)
    assert(schema.getField("event_id").schema().getLogicalType.getName == "uuid")
    assert(schema.getField("event_time").schema().getLogicalType.getName == "timestamp-micros")
    assert(
      schema.getField("ingestion_time").schema().getLogicalType.getName == "timestamp-micros"
    )

    val decimal = schema
      .getField("amount")
      .schema()
      .getLogicalType
      .asInstanceOf[LogicalTypes.Decimal]
    assert(decimal.getPrecision == 18)
    assert(decimal.getScale == 4)
    assert(schema.getField("transaction_type").schema().getType == Schema.Type.STRING)
  }

  test("an additive defaulted field is backward compatible with v1 data") {
    val writerSchema = loadedSchema
    val readerSchema = copyRecord(
      writerSchema,
      additionalFields = Vector(
        new Schema.Field(
          "optional_reference",
          Schema.createUnion(
            Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))
          ),
          "Test-only additive compatibility fixture.",
          JsonProperties.NULL_VALUE
        )
      )
    )

    val result = SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema)
    assert(result.getType == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)
  }

  test("an incompatible required field type change is detected") {
    val writerSchema = loadedSchema
    val readerSchema = copyRecord(
      writerSchema,
      replacements = Map("customer_id" -> Schema.create(Schema.Type.LONG))
    )

    val result = SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema)
    assert(result.getType == SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE)
  }

  private def copyRecord(
      source: Schema,
      replacements: Map[String, Schema] = Map.empty,
      additionalFields: Vector[Schema.Field] = Vector.empty
  ): Schema = {
    val copy = Schema.createRecord(source.getName, source.getDoc, source.getNamespace, false)
    val copiedFields = source.getFields.asScala.map { field =>
      new Schema.Field(
        field.name(),
        replacements.getOrElse(field.name(), field.schema()),
        field.doc(),
        field.defaultVal(),
        field.order()
      )
    }
    copy.setFields((copiedFields ++ additionalFields).asJava)
    copy
  }

  private def loadedSchema: Schema =
    TransactionEventAvroSchema.load match {
      case Right(schema) => schema
      case Left(error)   => fail(error.message)
    }
}
