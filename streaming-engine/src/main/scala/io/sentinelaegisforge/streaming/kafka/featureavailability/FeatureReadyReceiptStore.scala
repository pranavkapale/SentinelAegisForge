package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.{Files, Path}
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord

final case class FeatureReadyReceiptSummary(
    uniqueReceipts: Int,
    newObservations: Int,
    duplicateObservations: Int
)

/** Receipt-only research store. Receipt publication precedes Kafka offset commit. */
final class FeatureReadyReceiptStore(
    batch: FeatureReadyBatch,
    root: Path,
    group: String,
    topicIdentity: FeatureReadyTopicIdentity
) {
  private val receipts = root.resolve("receipts")
  private val observations = root.resolve("observations")
  private val deliveries = root.resolve("delivery_attempts")
  Files.createDirectories(receipts)
  Files.createDirectories(observations)
  Files.createDirectories(deliveries)
  private val binding = FeatureReadyCodec.objectBytes(
    "receipt_store_contract_version" -> "kafka-feature-ready-receipt-store-v1",
    "publication_batch_id" -> batch.batchId,
    "outbox_id" -> batch.outboxId,
    "group_id" -> group,
    "topic" -> batch.topic,
    "cluster_id" -> topicIdentity.clusterId,
    "topic_id" -> topicIdentity.topicId
  )
  FeatureReadyFiles.claim(root.resolve("receiver_identity.json"), binding)
  require(
    java.util.Arrays
      .equals(FeatureReadyFiles.read(root.resolve("receiver_identity.json")), binding),
    "Receiver/outbox/topic identity conflict"
  )
  inspect()

  private def receiptPath(id: String): Path = receipts.resolve(s"$id.json")

  def accept(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      failBeforeReceipt: Boolean = false
  ): Boolean = {
    require(record.topic() == batch.topic, "Unexpected Kafka topic")
    val event = FeatureReadyCodec.validateRecord(record.key(), record.value())
    val expected = batch.byReadyId.getOrElse(
      event.readyEventId,
      throw new IllegalArgumentException("Ready event not in verified outbox")
    )
    require(
      java.util.Arrays.equals(record.value(), expected.bytes),
      "Conflicting ready-event envelope/source-row fingerprint"
    )
    if (failBeforeReceipt) throw new IllegalStateException("Injected crash before durable receipt")
    val envelopeHash = FeatureReadyCodec.sha256(record.value())
    val path = receiptPath(event.readyEventId)
    val created = !Files.exists(path)
    if (created) {
      val receiptId = FeatureReadyCodec.sha256(
        FeatureReadyCodec.objectBytes(
          "ready_event_id" -> event.readyEventId,
          "outbox_id" -> batch.outboxId,
          "topic_id" -> topicIdentity.topicId,
          "partition" -> record.partition(),
          "offset" -> record.offset(),
          "envelope_fingerprint" -> envelopeHash
        )
      )
      FeatureReadyFiles.claim(
        path,
        FeatureReadyCodec.objectBytes(
          "receipt_contract_version" -> "kafka-feature-ready-receipt-v1",
          "receipt_id" -> receiptId,
          "ready_event_id" -> event.readyEventId,
          "event_id" -> event.eventId,
          "outbox_id" -> batch.outboxId,
          "topic" -> batch.topic,
          "topic_id" -> topicIdentity.topicId,
          "first_topic_partition" -> record.partition(),
          "first_topic_offset" -> record.offset(),
          "envelope_fingerprint" -> envelopeHash
        )
      )
    }
    verifyReceipt(path, event.readyEventId)
    val observation = FeatureReadyCodec.objectBytes(
      "observation_contract_version" -> "kafka-feature-ready-observation-v1",
      "ready_event_id" -> event.readyEventId,
      "event_id" -> event.eventId,
      "envelope_fingerprint" -> envelopeHash,
      "topic" -> batch.topic,
      "topic_id" -> topicIdentity.topicId,
      "partition" -> record.partition(),
      "offset" -> record.offset()
    )
    FeatureReadyFiles.claim(
      observations.resolve(s"${record.partition()}-${record.offset()}.json"),
      observation
    )
    val attemptId = UUID.randomUUID().toString
    FeatureReadyFiles.claim(
      deliveries.resolve(s"$attemptId.json"),
      FeatureReadyCodec.objectBytes(
        "attempt_id" -> attemptId,
        "ready_event_id" -> event.readyEventId,
        "partition" -> record.partition(),
        "offset" -> record.offset(),
        "duplicate" -> !created
      )
    )
    created
  }

  private def verifyReceipt(path: Path, readyId: String): Unit = {
    val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
    FeatureReadyCodec.requireFields(
      node,
      Set(
        "receipt_contract_version",
        "receipt_id",
        "ready_event_id",
        "event_id",
        "outbox_id",
        "topic",
        "topic_id",
        "first_topic_partition",
        "first_topic_offset",
        "envelope_fingerprint"
      )
    )
    val expected = batch.byReadyId.getOrElse(
      readyId,
      throw new IllegalArgumentException("Unknown persisted receipt")
    )
    require(
      FeatureReadyCodec.text(node, "receipt_contract_version") ==
        "kafka-feature-ready-receipt-v1" &&
        FeatureReadyCodec.text(node, "ready_event_id") == readyId &&
        FeatureReadyCodec.text(node, "event_id") == expected.event.eventId &&
        FeatureReadyCodec.text(node, "outbox_id") == batch.outboxId &&
        FeatureReadyCodec.text(node, "topic") == batch.topic &&
        FeatureReadyCodec.text(node, "topic_id") == topicIdentity.topicId &&
        FeatureReadyCodec.text(node, "envelope_fingerprint") ==
        FeatureReadyCodec.sha256(expected.bytes),
      "Persisted receipt/source mismatch"
    )
    val firstPartition = FeatureReadyCodec.integer(node, "first_topic_partition")
    val firstOffset = FeatureReadyCodec.integer(node, "first_topic_offset")
    require(
      firstPartition >= 0 && firstPartition < topicIdentity.partitions && firstOffset >= 0,
      "Invalid persisted first coordinates"
    )
    val expectedId = FeatureReadyCodec.sha256(
      FeatureReadyCodec.objectBytes(
        "ready_event_id" -> readyId,
        "outbox_id" -> batch.outboxId,
        "topic_id" -> topicIdentity.topicId,
        "partition" -> firstPartition,
        "offset" -> firstOffset,
        "envelope_fingerprint" -> FeatureReadyCodec.sha256(expected.bytes)
      )
    )
    require(FeatureReadyCodec.text(node, "receipt_id") == expectedId, "Receipt ID mismatch")
  }

  def inspect(): FeatureReadyReceiptSummary = {
    val receiptStream = Files.list(receipts)
    val unique =
      try
        receiptStream
          .iterator()
          .asScala
          .map { path =>
            require(path.getFileName.toString.endsWith(".json"), "Unexpected receipt file")
            val id = path.getFileName.toString.stripSuffix(".json")
            verifyReceipt(path, id)
            id
          }
          .toSet
      finally receiptStream.close()
    val observationStream = Files.list(observations)
    try
      observationStream.iterator().asScala.foreach { path =>
        require(path.getFileName.toString.endsWith(".json"), "Unexpected observation file")
        val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
        FeatureReadyCodec.requireFields(
          node,
          Set(
            "observation_contract_version",
            "ready_event_id",
            "event_id",
            "envelope_fingerprint",
            "topic",
            "topic_id",
            "partition",
            "offset"
          )
        )
        val id = FeatureReadyCodec.text(node, "ready_event_id")
        require(
          unique(id) &&
            FeatureReadyCodec.text(node, "observation_contract_version") ==
            "kafka-feature-ready-observation-v1" &&
            FeatureReadyCodec.text(node, "event_id") == batch.byReadyId(id).event.eventId &&
            FeatureReadyCodec.text(node, "topic") == batch.topic &&
            FeatureReadyCodec.text(node, "topic_id") == topicIdentity.topicId &&
            FeatureReadyCodec.text(node, "envelope_fingerprint") ==
            FeatureReadyCodec.sha256(batch.byReadyId(id).bytes) &&
            path.getFileName.toString ==
            s"${FeatureReadyCodec.integer(node, "partition")}-${FeatureReadyCodec.integer(node, "offset")}.json",
          "Invalid observed Kafka coordinate"
        )
      }
    finally observationStream.close()
    val deliveriesStream = Files.list(deliveries)
    val statuses =
      try
        deliveriesStream
          .iterator()
          .asScala
          .map { path =>
            require(path.getFileName.toString.endsWith(".json"), "Unexpected delivery attempt")
            val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
            FeatureReadyCodec.requireFields(
              node,
              Set("attempt_id", "ready_event_id", "partition", "offset", "duplicate")
            )
            val id = FeatureReadyCodec.text(node, "ready_event_id")
            require(
              unique(id) && path.getFileName.toString ==
                s"${FeatureReadyCodec.text(node, "attempt_id")}.json" &&
                Files.exists(
                  observations.resolve(
                    s"${FeatureReadyCodec.integer(node, "partition")}-" +
                      s"${FeatureReadyCodec.integer(node, "offset")}.json"
                  )
                ),
              "Invalid delivery attempt"
            )
            val duplicate = node.get("duplicate")
            require(duplicate != null && duplicate.isBoolean, "Invalid duplicate observation")
            duplicate.booleanValue()
          }
          .toVector
      finally deliveriesStream.close()
    FeatureReadyReceiptSummary(
      unique.size,
      statuses.count(value => !value),
      statuses.count(identity)
    )
  }

  def receiptIds: Set[String] = {
    inspect()
    val stream = Files.list(receipts)
    try stream.iterator().asScala.map(_.getFileName.toString.stripSuffix(".json")).toSet
    finally stream.close()
  }

  def observedCoordinates: Map[(Int, Long), String] = {
    inspect()
    val stream = Files.list(observations)
    try
      stream
        .iterator()
        .asScala
        .map { path =>
          val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
          (
            FeatureReadyCodec.integer(node, "partition").toInt,
            FeatureReadyCodec.integer(node, "offset")
          ) ->
            FeatureReadyCodec.text(node, "ready_event_id")
        }
        .toMap
    finally stream.close()
  }

  def receiptCoordinates: Map[String, (Int, Long)] = {
    inspect()
    val stream = Files.list(receipts)
    try
      stream
        .iterator()
        .asScala
        .map { path =>
          val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
          FeatureReadyCodec.text(node, "ready_event_id") ->
            (
              FeatureReadyCodec.integer(node, "first_topic_partition").toInt,
              FeatureReadyCodec.integer(node, "first_topic_offset")
            )
        }
        .toMap
    finally stream.close()
  }
}
