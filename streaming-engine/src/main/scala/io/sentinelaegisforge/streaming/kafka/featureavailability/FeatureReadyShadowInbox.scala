package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

/** Seals one fully reconciled research-group inbox; duplicate transport observations are not
  * identity.
  */
object FeatureReadyShadowInbox {
  val ContractVersion = "research-shadow-inbox-v1"
  val VerificationVersion = "research-shadow-inbox-verification-v1"
  val Group = "sentinel-feature-ready-shadow-v1"

  def sealReconciled(
      batch: FeatureReadyBatch,
      store: FeatureReadyReceiptStore,
      receiptsRoot: Path,
      identity: FeatureReadyTopicIdentity,
      reconciliation: FeatureReadyReconciliation,
      outputRoot: Path
  ): Path = {
    require(
      batch.records.size == reconciliation.sourceUnique &&
        reconciliation.consumerUniqueReceipts == batch.records.size &&
        store.receiptIds == batch.byReadyId.keySet,
      "Incomplete shadow receipt membership"
    )
    val raw = batch.records.flatMap(_.bytes).toArray
    val ids = new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode()
    batch.byReadyId.keys.toVector.sorted.foreach(ids.add)
    val receiptFiles = Files.list(receiptsRoot.resolve("receipts"))
    val inventory =
      try
        receiptFiles
          .iterator()
          .asScala
          .toVector
          .sortBy(_.getFileName.toString)
          .map(file =>
            s"${file.getFileName}:${FeatureReadyCodec.sha256(FeatureReadyFiles.read(file))}"
          )
      finally receiptFiles.close()
    require(inventory.size == batch.records.size, "Incomplete shadow receipt inventory")
    val inventoryNode = new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode()
    inventory.foreach(inventoryNode.add)
    val receiptHash = FeatureReadyCodec.sha256(FeatureReadyCodec.canonical(inventoryNode))
    val basis = Seq[(String, Any)](
      "inbox_contract_version" -> ContractVersion,
      "verification_contract_version" -> VerificationVersion,
      "publication_batch_id" -> batch.batchId,
      "outbox_id" -> batch.outboxId,
      "cluster_id" -> identity.clusterId,
      "topic_id" -> identity.topicId,
      "topic" -> batch.topic,
      "group_id" -> Group,
      "ready_event_count" -> batch.records.size,
      "ready_event_ids_sha256" -> FeatureReadyCodec.sha256(FeatureReadyCodec.canonical(ids)),
      "source_envelopes_sha256" -> FeatureReadyCodec.sha256(raw)
    )
    val inboxId = FeatureReadyCodec.sha256(FeatureReadyCodec.objectBytes(basis: _*))
    val target = outputRoot.resolve(inboxId)
    if (!Files.exists(target)) Files.createDirectories(target)
    val evidence = basis ++ Seq[(String, Any)](
      "inbox_id" -> inboxId,
      "batch_path" -> batch.path.toAbsolutePath.normalize().toString,
      "receipt_root" -> receiptsRoot.toAbsolutePath.normalize().toString,
      "receipt_inventory_sha256" -> receiptHash,
      "broker_physical_records" -> reconciliation.brokerPhysical,
      "broker_duplicate_records" -> reconciliation.brokerDuplicatePhysical,
      "consumer_duplicate_deliveries" -> reconciliation.consumerDuplicateDeliveries
    ) ++ (0 until identity.partitions).flatMap { partition =>
      Seq[(String, Any)](
        s"broker_beginning_$partition" -> reconciliation.brokerBeginning(partition),
        s"broker_end_$partition" -> reconciliation.brokerEnd(partition),
        s"group_committed_next_$partition" -> reconciliation.groupCommittedNext(partition)
      )
    }
    val manifest = target.resolve("inbox_manifest.json")
    val records = target.resolve("ready_events.jsonl")
    if (Files.exists(manifest) || Files.exists(records)) {
      require(Files.exists(manifest) && Files.exists(records), "Incomplete sealed inbox")
      val existing = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(manifest))
      basis.foreach { case (key, value) =>
        val actual = existing.get(key)
        require(
          actual != null && actual.asText() == value.toString,
          "Sealed inbox identity conflict"
        )
      }
      require(
        FeatureReadyCodec.text(existing, "receipt_inventory_sha256") == receiptHash &&
          java.util.Arrays.equals(FeatureReadyFiles.read(records), raw),
        "Sealed inbox source/receipt conflict"
      )
    } else {
      FeatureReadyFiles.claim(records, raw)
      FeatureReadyFiles.claim(manifest, FeatureReadyCodec.objectBytes(evidence: _*))
    }
    target
  }
}
