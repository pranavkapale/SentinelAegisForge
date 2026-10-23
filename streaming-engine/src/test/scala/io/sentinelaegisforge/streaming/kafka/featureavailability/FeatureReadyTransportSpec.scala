package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.{Files, Paths}

import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

final class FeatureReadyTransportSpec extends AnyFunSuite {
  private def batch: FeatureReadyBatch = {
    val bytes =
      Files.readAllBytes(Paths.get("..", "contracts", "test-fixtures", "feature-ready-v1.jsonl"))
    val event = FeatureReadyCodec.decode(bytes)
    FeatureReadyBatch(
      Paths.get("unused"),
      "batch-id",
      "outbox-id",
      FeatureReadyCodec.Topic,
      Vector(FeatureReadyRecord(event, bytes))
    )
  }
  private val topic = FeatureReadyTopicIdentity("cluster-id", "topic-id", 3)

  test("producer settings retain all-ack/idempotent bounded delivery") {
    val p = FeatureReadyKafkaConfig.producer("localhost:9092")
    assert(p.getProperty("acks") == "all")
    assert(p.getProperty("enable.idempotence") == "true")
    assert(p.getProperty("retries") == "5")
    assert(p.getProperty("max.in.flight.requests.per.connection") == "5")
    assert(p.getProperty("delivery.timeout.ms") == "120000")
    val c = FeatureReadyKafkaConfig.consumer("localhost:9092", "stable-group")
    assert(c.getProperty("enable.auto.commit") == "false")
    assert(c.getProperty("auto.offset.reset") == "earliest")
  }

  test("acknowledgement is mapped to identity and survives journal restart") {
    val root = Files.createTempDirectory("feature-ready-journal-")
    val source = batch
    val journal = new FeatureReadyPublicationJournal(source, root, topic)
    val producer = new MockProducer[Array[Byte], Array[Byte]](
      true,
      null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    assert(new FeatureReadyKafkaPublisher(producer, source, journal).publish() == 1)
    val id = source.records.head.event.readyEventId
    assert(journal.acknowledged(id).eventId == source.records.head.event.eventId)
    val restarted = new FeatureReadyPublicationJournal(source, root, topic)
    val secondProducer = new MockProducer[Array[Byte], Array[Byte]](
      true,
      null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    assert(new FeatureReadyKafkaPublisher(secondProducer, source, restarted).publish() == 0)
    assert(secondProducer.history().isEmpty)
    intercept[IllegalArgumentException] {
      new FeatureReadyPublicationJournal(source, root, topic.copy(topicId = "recreated-topic"))
    }
  }

  test("uncertain broker acknowledgement is retried; replay is explicit") {
    val root = Files.createTempDirectory("feature-ready-retry-")
    val source = batch
    val journal = new FeatureReadyPublicationJournal(source, root, topic)
    val first = new MockProducer[Array[Byte], Array[Byte]](
      true,
      null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    intercept[IllegalStateException] {
      new FeatureReadyKafkaPublisher(first, source, journal)
        .publish(failAfterBrokerAckBeforeJournalAt = 1)
    }
    assert(first.history().size() == 1)
    assert(journal.acknowledged.isEmpty)
    val second = new MockProducer[Array[Byte], Array[Byte]](
      true,
      null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    assert(new FeatureReadyKafkaPublisher(second, source, journal).publish() == 1)
    assert(journal.acknowledged.size == 1)
    val replay = new MockProducer[Array[Byte], Array[Byte]](
      true,
      null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    assert(
      new FeatureReadyKafkaPublisher(replay, source, journal)
        .publish(replayAcknowledged = true) == 1
    )
    assert(replay.history().size() == 1 && journal.acknowledged.size == 1)
  }

  test("durable logical receipt deduplicates identical redelivery and rejects conflicts") {
    val root = Files.createTempDirectory("feature-ready-receipts-")
    val source = batch
    val expected = source.records.head
    val record = new ConsumerRecord[Array[Byte], Array[Byte]](
      source.topic,
      0,
      4L,
      FeatureReadyCodec.key(expected.event),
      expected.bytes
    )
    val store = new FeatureReadyReceiptStore(source, root, "group", topic)
    intercept[IllegalStateException](store.accept(record, failBeforeReceipt = true))
    assert(store.receiptIds.isEmpty)
    assert(store.accept(record))
    val restarted = new FeatureReadyReceiptStore(source, root, "group", topic)
    assert(!restarted.accept(record))
    val summary = restarted.inspect()
    assert(summary.uniqueReceipts == 1)
    assert(summary.newObservations == 1 && summary.duplicateObservations == 1)
    val altered = FeatureReadyCodec.objectBytes(
      "availability_contract_version" -> expected.event.availabilityContractVersion,
      "ready_event_id" -> expected.event.readyEventId,
      "event_id" -> expected.event.eventId,
      "source_delta_table_id" -> expected.event.sourceDeltaTableId,
      "first_visible_delta_version" -> expected.event.firstVisibleDeltaVersion,
      "source_schema_fingerprint" -> "a" * 64,
      "source_feature_row_fingerprint" -> expected.event.sourceFeatureRowFingerprint,
      "availability_state" -> "DELTA_COMMITTED"
    )
    val conflicting = new ConsumerRecord[Array[Byte], Array[Byte]](
      source.topic,
      0,
      5L,
      FeatureReadyCodec.key(expected.event),
      altered
    )
    intercept[IllegalArgumentException](restarted.accept(conflicting))
    assert(restarted.receiptIds.size == 1)
  }

  test("shadow inbox seals only complete dedicated-group receipts and retains semantic identity") {
    val source = batch
    val root = Files.createTempDirectory("feature-ready-shadow-seal-")
    val receipts = root.resolve("receipts")
    val store = new FeatureReadyReceiptStore(source, receipts, FeatureReadyShadowInbox.Group, topic)
    val reconciliation = FeatureReadyReconciliation(
      1,
      1,
      1,
      0,
      1,
      0,
      Map(0 -> 1),
      Map(0 -> 0L, 1 -> 0L, 2 -> 0L),
      Map(0 -> 1L, 1 -> 0L, 2 -> 0L),
      Map(0 -> 1L, 1 -> 0L, 2 -> 0L)
    )
    intercept[IllegalArgumentException] {
      FeatureReadyShadowInbox.sealReconciled(
        source,
        store,
        receipts,
        topic,
        reconciliation,
        root.resolve("inboxes")
      )
    }
    val event = source.records.head
    val record = new ConsumerRecord[Array[Byte], Array[Byte]](
      source.topic,
      0,
      0L,
      FeatureReadyCodec.key(event.event),
      event.bytes
    )
    assert(store.accept(record))
    val first = FeatureReadyShadowInbox.sealReconciled(
      source,
      store,
      receipts,
      topic,
      reconciliation,
      root.resolve("inboxes")
    )
    assert(Files.isRegularFile(first.resolve("inbox_manifest.json")))
    assert(
      java.util.Arrays.equals(Files.readAllBytes(first.resolve("ready_events.jsonl")), event.bytes)
    )
    assert(!store.accept(record))
    val replay = reconciliation.copy(brokerPhysical = 2, brokerDuplicatePhysical = 1)
    assert(
      FeatureReadyShadowInbox.sealReconciled(
        source,
        store,
        receipts,
        topic,
        replay,
        root.resolve("inboxes")
      ) == first
    )
    val manifest =
      FeatureReadyCodec.parseJson(Files.readAllBytes(first.resolve("inbox_manifest.json")))
    assert(FeatureReadyCodec.text(manifest, "group_id") == FeatureReadyShadowInbox.Group)
    assert(FeatureReadyCodec.integer(manifest, "broker_physical_records") == 1L)
  }

  test("publication batch byte hash and source-outbox binding fail closed") {
    val source = batch
    val root = Files.createTempDirectory("feature-ready-batch-")
    val outbox = Files.createDirectory(root.resolve("outbox"))
    val outboxManifest = FeatureReadyCodec.objectBytes("outbox_id" -> "outbox-id")
    Files.write(outbox.resolve("outbox_manifest.json"), outboxManifest)
    Files.write(outbox.resolve("ready_events.jsonl"), source.records.head.bytes)
    val ids = new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode()
    ids.add(source.records.head.event.readyEventId)
    val fields: Seq[(String, Any)] = Seq(
      "publication_batch_contract_version" -> FeatureReadyBatch.ContractVersion,
      "wire_contract_version" -> FeatureReadyCodec.WireVersion,
      "topic" -> FeatureReadyCodec.Topic,
      "outbox_path" -> outbox.toAbsolutePath.toString,
      "outbox_id" -> "outbox-id",
      "outbox_manifest_sha256" -> FeatureReadyCodec.sha256(outboxManifest),
      "source_delta_table_id" -> source.records.head.event.sourceDeltaTableId,
      "source_schema_fingerprint" -> source.records.head.event.sourceSchemaFingerprint,
      "start_delta_version" -> 0,
      "end_delta_version" -> 0,
      "ready_event_count" -> 1,
      "ready_event_ids_sha256" -> FeatureReadyCodec.sha256(FeatureReadyCodec.canonical(ids)),
      "records_sha256" -> FeatureReadyCodec.sha256(source.records.head.bytes),
      "maximum_record_bytes" -> FeatureReadyCodec.MaximumRecordBytes
    )
    val id = FeatureReadyCodec.sha256(FeatureReadyCodec.objectBytes(fields: _*))
    val directory = Files.createDirectory(root.resolve(id))
    Files.write(
      directory.resolve("publish_batch_manifest.json"),
      FeatureReadyCodec.objectBytes((fields :+ ("publication_batch_id" -> id)): _*)
    )
    val recordsPath = directory.resolve("records.jsonl")
    Files.write(recordsPath, source.records.head.bytes)
    assert(FeatureReadyBatch.load(directory).records.size == 1)
    Files.write(recordsPath, "{}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    intercept[IllegalArgumentException](FeatureReadyBatch.load(directory))
  }

  test("receipt-store corruption fails inspection instead of silently reusing the identity") {
    val source = batch
    val root = Files.createTempDirectory("feature-ready-corrupt-")
    val store = new FeatureReadyReceiptStore(source, root, "group", topic)
    val expected = source.records.head
    val record = new ConsumerRecord[Array[Byte], Array[Byte]](
      source.topic,
      0,
      0L,
      FeatureReadyCodec.key(expected.event),
      expected.bytes
    )
    assert(store.accept(record))
    Files.write(
      root.resolve("receipts").resolve(s"${expected.event.readyEventId}.json"),
      "{}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    )
    intercept[IllegalArgumentException] {
      new FeatureReadyReceiptStore(source, root, "group", topic)
    }
  }

  private final class RecordingConsumer extends MockConsumer[Array[Byte], Array[Byte]]("earliest") {
    var committedProgress = Vector.empty[Map[Int, Long]]
    override def commitSync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
      super.commitSync(offsets)
      committedProgress :+= offsets.asScala.toMap.map { case (tp, position) =>
        tp.partition() -> position.offset()
      }
    }
  }

  private def mockConsumer(source: FeatureReadyBatch): RecordingConsumer = {
    val consumer = new RecordingConsumer
    val partitions = (0 until 3).map(new TopicPartition(source.topic, _)).toVector
    consumer.updateBeginningOffsets(partitions.map(_ -> java.lang.Long.valueOf(0L)).toMap.asJava)
    consumer.updateEndOffsets(
      partitions
        .map { tp =>
          tp -> java.lang.Long.valueOf(if (tp.partition() == 0) 1L else 0L)
        }
        .toMap
        .asJava
    )
    consumer.schedulePollTask(() => {
      consumer.rebalance(partitions.asJava)
      val event = source.records.head
      consumer.addRecord(
        new ConsumerRecord[Array[Byte], Array[Byte]](
          source.topic,
          0,
          0L,
          FeatureReadyCodec.key(event.event),
          event.bytes
        )
      )
    })
    consumer
  }

  test("manual next-offset commit follows durable receipt, never precedes it") {
    val source = batch
    val store = new FeatureReadyReceiptStore(
      source,
      Files.createTempDirectory("feature-ready-commit-"),
      "group",
      topic
    )
    val consumer = mockConsumer(source)
    val result = new FeatureReadyKafkaConsumer(consumer, source, store).consume(1)
    assert(result.examined == 1 && result.committedNext(0) == 1L)
    assert(store.receiptIds == Set(source.records.head.event.readyEventId))
    assert(consumer.committedProgress == Vector(Map(0 -> 1L)))
  }

  test("before-receipt crash does not commit; after-receipt crash deduplicates on restart") {
    val source = batch
    val store = new FeatureReadyReceiptStore(
      source,
      Files.createTempDirectory("feature-ready-crash-"),
      "group",
      topic
    )
    val before = mockConsumer(source)
    intercept[IllegalStateException] {
      new FeatureReadyKafkaConsumer(before, source, store).consume(1, failBeforeReceiptAt = 1)
    }
    assert(store.receiptIds.isEmpty)
    assert(before.committedProgress.isEmpty)
    val after = mockConsumer(source)
    intercept[IllegalStateException] {
      new FeatureReadyKafkaConsumer(after, source, store)
        .consume(1, failAfterReceiptBeforeCommitAt = 1)
    }
    assert(store.receiptIds.size == 1)
    assert(after.committedProgress.isEmpty)
    val resumed = mockConsumer(source)
    val report = new FeatureReadyKafkaConsumer(resumed, source, store).consume(1)
    assert(report.receipts.uniqueReceipts == 1)
    assert(report.receipts.duplicateObservations == 1)
    assert(report.committedNext(0) == 1L)
    assert(resumed.committedProgress == Vector(Map(0 -> 1L)))
  }

  test("Kafka progress remains partition-scoped while logical receipts deduplicate") {
    val source = batch
    val store = new FeatureReadyReceiptStore(
      source,
      Files.createTempDirectory("feature-ready-partitions-"),
      "group",
      topic
    )
    val consumer = new RecordingConsumer
    val partitions = (0 until 3).map(new TopicPartition(source.topic, _)).toVector
    consumer.updateBeginningOffsets(partitions.map(_ -> java.lang.Long.valueOf(0L)).toMap.asJava)
    consumer.updateEndOffsets(
      partitions
        .map { tp =>
          tp -> java.lang.Long.valueOf(if (tp.partition() < 2) 1L else 0L)
        }
        .toMap
        .asJava
    )
    consumer.schedulePollTask(() => {
      consumer.rebalance(partitions.asJava)
      val event = source.records.head
      (0 until 2).foreach { partition =>
        consumer.addRecord(
          new ConsumerRecord[Array[Byte], Array[Byte]](
            source.topic,
            partition,
            0L,
            FeatureReadyCodec.key(event.event),
            event.bytes
          )
        )
      }
    })
    val report = new FeatureReadyKafkaConsumer(consumer, source, store).consume(2)
    assert(report.examined == 2)
    assert(report.committedNext == Map(0 -> 1L, 1 -> 1L, 2 -> 0L))
    assert(consumer.committedProgress.toSet == Set(Map(0 -> 1L), Map(1 -> 1L)))
    assert(report.receipts.uniqueReceipts == 1)
    assert(report.receipts.duplicateObservations == 1)
  }
}
