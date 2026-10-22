package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}

/** Strict cross-language feature-availability-v1 Kafka value, not a feature row. */
final case class FeatureReadyEnvelope(
    availabilityContractVersion: String,
    readyEventId: String,
    eventId: String,
    sourceDeltaTableId: String,
    firstVisibleDeltaVersion: Long,
    sourceSchemaFingerprint: String,
    sourceFeatureRowFingerprint: String,
    availabilityState: String
)

object FeatureReadyCodec {
  val WireVersion = "feature-ready-kafka-wire-v1"
  val Topic = "features.ready"
  val MaximumRecordBytes = 4096
  private val hex64 = "[0-9a-f]{64}".r
  private val keys = Set(
    "availability_contract_version",
    "ready_event_id",
    "event_id",
    "source_delta_table_id",
    "first_visible_delta_version",
    "source_schema_fingerprint",
    "source_feature_row_fingerprint",
    "availability_state"
  )
  private val mapper = new ObjectMapper()
  mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
  mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)

  def sha256(raw: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(raw).map(b => f"${b & 0xff}%02x").mkString

  def canonical(node: JsonNode): Array[Byte] = {
    def ordered(value: JsonNode): AnyRef =
      if (value.isObject) {
        val result = new java.util.TreeMap[String, AnyRef]()
        value
          .properties()
          .asScala
          .foreach(entry => result.put(entry.getKey, ordered(entry.getValue)))
        result
      } else if (value.isArray) {
        value.elements().asScala.map(ordered).toVector.asJava
      } else if (value.isTextual) value.textValue()
      else if (value.isIntegralNumber) java.lang.Long.valueOf(value.longValue())
      else if (value.isBoolean) java.lang.Boolean.valueOf(value.booleanValue())
      else if (value.isNull) null
      else throw new IllegalArgumentException("Unsupported canonical JSON value")
    (mapper.writeValueAsString(ordered(node)) + "\n").getBytes(StandardCharsets.UTF_8)
  }

  def objectBytes(fields: (String, Any)*): Array[Byte] = {
    val node = mapper.createObjectNode()
    fields.foreach {
      case (name, value: String)  => node.put(name, value)
      case (name, value: Int)     => node.put(name, value)
      case (name, value: Long)    => node.put(name, value)
      case (name, value: Boolean) => node.put(name, value)
      case (name, null)           => node.putNull(name)
      case (name, _) => throw new IllegalArgumentException(s"Unsupported JSON field $name")
    }
    canonical(node)
  }

  def parseJson(raw: Array[Byte]): JsonNode = {
    val node = mapper.readTree(raw)
    require(node != null && java.util.Arrays.equals(raw, canonical(node)), "Noncanonical JSON")
    node
  }

  def text(node: JsonNode, name: String): String = {
    val value = node.get(name)
    require(value != null && value.isTextual, s"Invalid $name")
    value.textValue()
  }

  def integer(node: JsonNode, name: String): Long = {
    val value = node.get(name)
    require(value != null && value.isIntegralNumber && value.canConvertToLong, s"Invalid $name")
    value.longValue()
  }

  def requireFields(node: JsonNode, expected: Set[String]): Unit =
    require(node.isObject && node.fieldNames().asScala.toSet == expected, "Unexpected JSON fields")

  def decode(raw: Array[Byte]): FeatureReadyEnvelope = {
    require(raw != null && raw.length <= MaximumRecordBytes, "Invalid feature-ready record size")
    val node = parseJson(raw)
    requireFields(node, keys)
    val event = FeatureReadyEnvelope(
      text(node, "availability_contract_version"),
      text(node, "ready_event_id"),
      text(node, "event_id"),
      text(node, "source_delta_table_id"),
      integer(node, "first_visible_delta_version"),
      text(node, "source_schema_fingerprint"),
      text(node, "source_feature_row_fingerprint"),
      text(node, "availability_state")
    )
    require(
      event.availabilityContractVersion == "feature-availability-v1",
      "Unsupported availability version"
    )
    require(event.availabilityState == "DELTA_COMMITTED", "Unsupported availability state")
    require(event.firstVisibleDeltaVersion >= 0, "Negative Delta version")
    require(
      UUID.fromString(event.eventId).toString == event.eventId &&
        UUID.fromString(event.sourceDeltaTableId).toString == event.sourceDeltaTableId,
      "Noncanonical UUID"
    )
    require(
      Seq(event.readyEventId, event.sourceSchemaFingerprint, event.sourceFeatureRowFingerprint)
        .forall(value => hex64.matches(value)),
      "Invalid SHA-256 fingerprint"
    )
    val identity = mapper.createArrayNode()
    identity.add(event.availabilityContractVersion)
    identity.add(event.sourceDeltaTableId)
    identity.add(event.eventId)
    identity.add(event.firstVisibleDeltaVersion)
    identity.add(event.sourceFeatureRowFingerprint)
    require(event.readyEventId == sha256(canonical(identity)), "Ready-event identity mismatch")
    event
  }

  def key(event: FeatureReadyEnvelope): Array[Byte] =
    event.readyEventId.getBytes(StandardCharsets.UTF_8)

  def validateRecord(keyBytes: Array[Byte], valueBytes: Array[Byte]): FeatureReadyEnvelope = {
    val event = decode(valueBytes)
    require(keyBytes != null && java.util.Arrays.equals(keyBytes, key(event)), "Kafka key mismatch")
    event
  }
}
