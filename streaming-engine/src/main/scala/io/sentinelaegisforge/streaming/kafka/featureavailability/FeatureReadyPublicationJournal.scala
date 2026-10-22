package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.{Files, Path}
import java.util.UUID

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode

final case class FeatureReadyAcknowledgement(
    readyEventId: String,
    eventId: String,
    topic: String,
    partition: Int,
    offset: Long,
    attemptId: String
)

final class FeatureReadyPublicationJournal(
    batch: FeatureReadyBatch,
    root: Path,
    topicIdentity: FeatureReadyTopicIdentity
) {
  private val directory = root.resolve(batch.batchId)
  private val acks = directory.resolve("acknowledgements")
  private val attempts = directory.resolve("attempts")
  private val identity = FeatureReadyCodec.objectBytes(
    "journal_contract_version" -> "feature-ready-kafka-publication-journal-v1",
    "publication_batch_id" -> batch.batchId,
    "outbox_id" -> batch.outboxId,
    "topic" -> batch.topic,
    "cluster_id" -> topicIdentity.clusterId,
    "topic_id" -> topicIdentity.topicId
  )
  Files.createDirectories(acks)
  Files.createDirectories(attempts)
  FeatureReadyFiles.claim(directory.resolve("journal_identity.json"), identity)
  require(
    java.util.Arrays
      .equals(FeatureReadyFiles.read(directory.resolve("journal_identity.json")), identity),
    "Publication journal/topic identity conflict"
  )

  def acknowledged: Map[String, FeatureReadyAcknowledgement] = {
    val stream = Files.list(acks)
    try
      stream
        .iterator()
        .asScala
        .map { path =>
          require(
            !Files.isSymbolicLink(path) && path.getFileName.toString.endsWith(".json"),
            "Unexpected acknowledgement artifact"
          )
          val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
          val ack = parseAck(node)
          require(
            path.getFileName.toString == s"${ack.readyEventId}.json",
            "Misnamed acknowledgement"
          )
          ack.readyEventId -> ack
        }
        .toMap
    finally stream.close()
  }

  private def parseAck(node: JsonNode): FeatureReadyAcknowledgement = {
    FeatureReadyCodec.requireFields(
      node,
      Set(
        "ack_contract_version",
        "publication_batch_id",
        "outbox_id",
        "cluster_id",
        "topic_id",
        "ready_event_id",
        "event_id",
        "topic",
        "partition",
        "offset",
        "attempt_id"
      )
    )
    require(
      FeatureReadyCodec.text(node, "ack_contract_version") ==
        "feature-ready-kafka-ack-v1",
      "Unsupported acknowledgement version"
    )
    require(
      FeatureReadyCodec.text(node, "publication_batch_id") == batch.batchId &&
        FeatureReadyCodec.text(node, "outbox_id") == batch.outboxId &&
        FeatureReadyCodec.text(node, "cluster_id") == topicIdentity.clusterId &&
        FeatureReadyCodec.text(node, "topic_id") == topicIdentity.topicId,
      "Acknowledgement lineage mismatch"
    )
    val readyId = FeatureReadyCodec.text(node, "ready_event_id")
    val eventId = FeatureReadyCodec.text(node, "event_id")
    require(
      batch.byReadyId.get(readyId).exists(_.event.eventId == eventId),
      "Unknown acknowledgement event"
    )
    val topic = FeatureReadyCodec.text(node, "topic")
    val partitionLong = FeatureReadyCodec.integer(node, "partition")
    val offset = FeatureReadyCodec.integer(node, "offset")
    require(
      topic == batch.topic && partitionLong >= 0 &&
        partitionLong < topicIdentity.partitions &&
        offset >= 0,
      "Invalid acknowledgement coordinates"
    )
    FeatureReadyAcknowledgement(
      readyId,
      eventId,
      topic,
      partitionLong.toInt,
      offset,
      FeatureReadyCodec.text(node, "attempt_id")
    )
  }

  def start(replay: Boolean): String = {
    inspectAttempts()
    acknowledged
    val id = UUID.randomUUID().toString // Operational attempt only; never source identity.
    val path = attempts.resolve(id)
    Files.createDirectory(path)
    Files.createDirectory(path.resolve("deliveries"))
    FeatureReadyFiles.claim(
      path.resolve("start.json"),
      FeatureReadyCodec.objectBytes(
        "attempt_id" -> id,
        "publication_batch_id" -> batch.batchId,
        "replay_acknowledged" -> replay
      )
    )
    id
  }

  def record(
      attemptId: String,
      record: FeatureReadyRecord,
      partition: Int,
      offset: Long,
      replay: Boolean
  ): Unit = {
    val bytes = FeatureReadyCodec.objectBytes(
      "ack_contract_version" -> "feature-ready-kafka-ack-v1",
      "publication_batch_id" -> batch.batchId,
      "outbox_id" -> batch.outboxId,
      "cluster_id" -> topicIdentity.clusterId,
      "topic_id" -> topicIdentity.topicId,
      "ready_event_id" -> record.event.readyEventId,
      "event_id" -> record.event.eventId,
      "topic" -> batch.topic,
      "partition" -> partition,
      "offset" -> offset,
      "attempt_id" -> attemptId
    )
    FeatureReadyFiles.claim(
      attempts
        .resolve(attemptId)
        .resolve("deliveries")
        .resolve(s"${record.event.readyEventId}.json"),
      bytes
    )
    if (!replay) FeatureReadyFiles.claim(acks.resolve(s"${record.event.readyEventId}.json"), bytes)
  }

  def complete(attemptId: String, delivered: Int, replay: Boolean): Unit = {
    FeatureReadyFiles.claim(
      attempts.resolve(attemptId).resolve("complete.json"),
      FeatureReadyCodec.objectBytes(
        "attempt_id" -> attemptId,
        "delivered" -> delivered,
        "replay_acknowledged" -> replay
      )
    )
  }

  def summary: String = {
    val count = acknowledged.size
    val (complete, incomplete) = inspectAttempts()
    s"batch=${batch.batchId} acknowledged=$count/${batch.records.size} " +
      s"publication_complete=${count == batch.records.size} " +
      s"completed_attempts=$complete incomplete_attempts=$incomplete topic_id=${topicIdentity.topicId}"
  }

  private def inspectAttempts(): (Int, Int) = {
    val stream = Files.list(attempts)
    try {
      val all = stream.iterator().asScala.toVector
      all.foreach { path =>
        require(
          !Files.isSymbolicLink(path) && Files.isDirectory(path),
          "Unexpected publication attempt artifact"
        )
        val names = Files.list(path)
        try {
          val present = names.iterator().asScala.map(_.getFileName.toString).toSet
          require(
            present == Set("start.json", "deliveries") ||
              present == Set("start.json", "deliveries", "complete.json"),
            "Incomplete publication attempt structure"
          )
        } finally names.close()
        val start = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path.resolve("start.json")))
        FeatureReadyCodec.requireFields(
          start,
          Set("attempt_id", "publication_batch_id", "replay_acknowledged")
        )
        require(
          FeatureReadyCodec.text(start, "attempt_id") == path.getFileName.toString &&
            FeatureReadyCodec.text(start, "publication_batch_id") == batch.batchId &&
            start.get("replay_acknowledged").isBoolean,
          "Publication attempt identity mismatch"
        )
        val deliveries = path.resolve("deliveries")
        require(
          !Files.isSymbolicLink(deliveries) && Files.isDirectory(deliveries),
          "Invalid publication delivery directory"
        )
        val deliveryStream = Files.list(deliveries)
        val count =
          try
            deliveryStream
              .iterator()
              .asScala
              .map { file =>
                val ack = parseAck(FeatureReadyCodec.parseJson(FeatureReadyFiles.read(file)))
                require(
                  file.getFileName.toString == s"${ack.readyEventId}.json" &&
                    ack.attemptId == path.getFileName.toString,
                  "Publication delivery mismatch"
                )
                ack.readyEventId
              }
              .toVector
              .size
          finally deliveryStream.close()
        val completion = path.resolve("complete.json")
        if (Files.exists(completion)) {
          val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(completion))
          FeatureReadyCodec.requireFields(
            node,
            Set("attempt_id", "delivered", "replay_acknowledged")
          )
          require(
            FeatureReadyCodec.text(node, "attempt_id") == path.getFileName.toString &&
              FeatureReadyCodec.integer(node, "delivered") == count &&
              node.get("replay_acknowledged").isBoolean &&
              node.get("replay_acknowledged").booleanValue() ==
              start.get("replay_acknowledged").booleanValue(),
            "Publication attempt completion mismatch"
          )
        }
      }
      (
        all.count(path => Files.exists(path.resolve("complete.json"))),
        all.count(path => !Files.exists(path.resolve("complete.json")))
      )
    } finally stream.close()
  }
}
