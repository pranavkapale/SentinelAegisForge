package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode

final case class FeatureReadyRecord(event: FeatureReadyEnvelope, bytes: Array[Byte])

final case class FeatureReadyBatch(
    path: Path,
    batchId: String,
    outboxId: String,
    topic: String,
    records: Vector[FeatureReadyRecord]
) {
  val byReadyId: Map[String, FeatureReadyRecord] =
    records.map(record => record.event.readyEventId -> record).toMap
}

object FeatureReadyBatch {
  val ContractVersion = "feature-ready-kafka-publish-batch-v1"
  val MaximumEvents = 5000

  def load(path: Path): FeatureReadyBatch = {
    require(!Files.isSymbolicLink(path) && Files.isDirectory(path), "Invalid batch directory")
    val names = Files.list(path)
    try
      require(
        names.iterator().asScala.map(_.getFileName.toString).toSet ==
          Set("publish_batch_manifest.json", "records.jsonl"),
        "Unexpected batch artifacts"
      )
    finally names.close()
    val manifestPath = path.resolve("publish_batch_manifest.json")
    val recordsPath = path.resolve("records.jsonl")
    require(
      !Files.isSymbolicLink(manifestPath) && !Files.isSymbolicLink(recordsPath),
      "Linked batch artifact"
    )
    val manifest = FeatureReadyCodec.parseJson(Files.readAllBytes(manifestPath))
    FeatureReadyCodec.requireFields(
      manifest,
      Set(
        "publication_batch_contract_version",
        "wire_contract_version",
        "topic",
        "outbox_path",
        "outbox_id",
        "outbox_manifest_sha256",
        "source_delta_table_id",
        "source_schema_fingerprint",
        "start_delta_version",
        "end_delta_version",
        "ready_event_count",
        "ready_event_ids_sha256",
        "records_sha256",
        "maximum_record_bytes",
        "publication_batch_id"
      )
    )
    val basis = manifest.deepCopy().asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    basis.remove("publication_batch_id")
    val batchId = FeatureReadyCodec.text(manifest, "publication_batch_id")
    require(
      batchId == FeatureReadyCodec.sha256(FeatureReadyCodec.canonical(basis)) &&
        path.getFileName.toString == batchId,
      "Publication batch identity mismatch"
    )
    require(
      FeatureReadyCodec.text(manifest, "publication_batch_contract_version") == ContractVersion,
      "Unsupported publication batch version"
    )
    require(
      FeatureReadyCodec.text(manifest, "wire_contract_version") == FeatureReadyCodec.WireVersion,
      "Unsupported Kafka wire version"
    )
    val topic = FeatureReadyCodec.text(manifest, "topic")
    require(topic == FeatureReadyCodec.Topic, "Unexpected feature-ready topic")
    require(
      FeatureReadyCodec
        .integer(manifest, "maximum_record_bytes") == FeatureReadyCodec.MaximumRecordBytes,
      "Record-size policy mismatch"
    )
    val count = FeatureReadyCodec.integer(manifest, "ready_event_count")
    require(count >= 1 && count <= MaximumEvents, "Invalid publication batch size")
    require(
      Files.size(recordsPath) <= MaximumEvents.toLong * FeatureReadyCodec.MaximumRecordBytes,
      "Publication batch bytes exceed local bound"
    )
    val raw = Files.readAllBytes(recordsPath)
    require(
      FeatureReadyCodec.sha256(raw) == FeatureReadyCodec.text(manifest, "records_sha256"),
      "Publication batch byte hash mismatch"
    )
    val content = new String(raw, StandardCharsets.UTF_8)
    require(content.endsWith("\n"), "Unterminated publication JSONL")
    val records = content.split("\n", -1).dropRight(1).toVector.map { line =>
      val bytes = (line + "\n").getBytes(StandardCharsets.UTF_8)
      FeatureReadyRecord(FeatureReadyCodec.decode(bytes), bytes)
    }
    require(
      java.util.Arrays.equals(raw, records.flatMap(_.bytes).toArray),
      "Publication batch contains invalid UTF-8 or altered line bytes"
    )
    require(records.size == count, "Publication batch count mismatch")
    val readyIds = records.map(_.event.readyEventId)
    require(readyIds.distinct.size == records.size, "Duplicate ready-event ID in batch")
    val idsNode = new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode()
    readyIds.sorted.foreach(idsNode.add)
    require(
      FeatureReadyCodec.sha256(FeatureReadyCodec.canonical(idsNode)) ==
        FeatureReadyCodec.text(manifest, "ready_event_ids_sha256"),
      "Ready-event set hash mismatch"
    )
    val outboxId = FeatureReadyCodec.text(manifest, "outbox_id")
    val tableId = FeatureReadyCodec.text(manifest, "source_delta_table_id")
    val schema = FeatureReadyCodec.text(manifest, "source_schema_fingerprint")
    val start = FeatureReadyCodec.integer(manifest, "start_delta_version")
    val end = FeatureReadyCodec.integer(manifest, "end_delta_version")
    require(
      records.forall(record =>
        record.event.sourceDeltaTableId == tableId &&
          record.event.sourceSchemaFingerprint == schema &&
          record.event.firstVisibleDeltaVersion >= start &&
          record.event.firstVisibleDeltaVersion <= end
      ),
      "Batch/source identity mismatch"
    )
    val outbox = Path.of(FeatureReadyCodec.text(manifest, "outbox_path"))
    require(!Files.isSymbolicLink(outbox) && Files.isDirectory(outbox), "Missing source outbox")
    val sourceManifest = outbox.resolve("outbox_manifest.json")
    val sourceRecords = outbox.resolve("ready_events.jsonl")
    require(
      !Files.isSymbolicLink(sourceManifest) && !Files.isSymbolicLink(sourceRecords),
      "Linked outbox source artifact"
    )
    require(
      FeatureReadyCodec.sha256(Files.readAllBytes(sourceManifest)) ==
        FeatureReadyCodec.text(manifest, "outbox_manifest_sha256") &&
        java.util.Arrays.equals(raw, Files.readAllBytes(sourceRecords)),
      "Batch differs from source outbox bytes"
    )
    val source = FeatureReadyCodec.parseJson(Files.readAllBytes(sourceManifest))
    require(FeatureReadyCodec.text(source, "outbox_id") == outboxId, "Outbox identity mismatch")
    FeatureReadyBatch(path, batchId, outboxId, topic, records)
  }
}
