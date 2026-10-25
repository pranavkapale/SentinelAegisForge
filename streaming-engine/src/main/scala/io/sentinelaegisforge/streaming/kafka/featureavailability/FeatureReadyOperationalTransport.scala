package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.Path
import java.time.Instant
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

/** An isolated, sequential local-research lineage over the existing features.ready topic. */
object FeatureReadyOperationalTransport {
  val Group = "sentinel-feature-ready-incremental-shadow-v1"
  val FenceVersion = "feature-ready-operational-fence-v1"
  val AcceptanceVersion = "feature-ready-operational-acceptance-v1"

  private def partitions: Vector[TopicPartition] =
    (0 until 3).map(new TopicPartition(FeatureReadyCodec.Topic, _)).toVector

  def requireOperationalGroup(group: String): Unit =
    require(
      group == Group ||
        (group.startsWith(s"$Group-") && group.length <= 120 &&
          group.forall(ch =>
            (ch >= 'a' && ch <= 'z') ||
              (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '-'
          )),
      "Invalid isolated operational group"
    )

  private def offsets(
      node: com.fasterxml.jackson.databind.JsonNode,
      prefix: String
  ): Map[Int, Long] =
    (0 until 3).map { partition =>
      partition -> FeatureReadyCodec.integer(node, s"${prefix}_$partition")
    }.toMap

  def boundary(
      path: Path,
      identity: FeatureReadyTopicIdentity,
      group: String = Group
  ): Map[Int, Long] = {
    requireOperationalGroup(group)
    val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(path))
    require(
      FeatureReadyCodec.text(node, "group_id") == group &&
        FeatureReadyCodec.text(node, "cluster_id") == identity.clusterId &&
        FeatureReadyCodec.text(node, "topic_id") == identity.topicId &&
        FeatureReadyCodec.text(node, "topic") == FeatureReadyCodec.Topic,
      "Operational boundary group/topic identity mismatch"
    )
    val version = FeatureReadyCodec.text(node, "contract_version")
    require(
      version == FenceVersion || version == AcceptanceVersion,
      "Unsupported operational offset boundary"
    )
    val expected = Set("contract_version", "group_id", "cluster_id", "topic_id", "topic") ++
      (0 until 3).map(p => s"end_$p") ++
      (if (version == FenceVersion)
         (0 until 3).map(p => s"beginning_$p")
       else
         Set("publication_batch_id", "outbox_id", "ready_event_count", "physical_record_count") ++
           (0 until 3).map(p => s"start_$p"))
    FeatureReadyCodec.requireFields(node, expected)
    val ends = offsets(node, "end")
    val starts = offsets(node, if (version == FenceVersion) "beginning" else "start")
    require(
      ends.forall { case (partition, end) =>
        starts(partition) >= 0 && starts(partition) <= end
      },
      "Invalid operational offset range"
    )
    ends
  }

  /** Claim the durable fence before advancing a previously unused stable group. */
  def initialize(
      bootstrap: String,
      identity: FeatureReadyTopicIdentity,
      path: Path,
      group: String = Group
  ): Map[Int, Long] = {
    requireOperationalGroup(group)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      FeatureReadyKafkaConfig.consumer(bootstrap, group)
    )
    try {
      consumer.assign(partitions.asJava)
      val committed = consumer.committed(partitions.toSet.asJava).asScala.toMap
      if (java.nio.file.Files.exists(path)) {
        val end = boundary(path, identity, group)
        require(
          partitions.forall { tp =>
            Option(committed(tp)).forall(_.offset() >= end(tp.partition()))
          },
          "Existing operational group precedes its initial fence"
        )
        if (committed.values.forall(_ == null))
          consumer.commitSync(
            partitions
              .map { tp =>
                tp -> new OffsetAndMetadata(end(tp.partition()))
              }
              .toMap
              .asJava
          )
        else require(committed.values.forall(_ != null), "Partially initialized group")
        end
      } else {
        require(committed.values.forall(_ == null), "Operational group already has offsets")
        val beginning = consumer.beginningOffsets(partitions.asJava).asScala.toMap
        val end = consumer.endOffsets(partitions.asJava).asScala.toMap
        val fields = Vector[(String, Any)](
          "contract_version" -> FenceVersion,
          "group_id" -> group,
          "cluster_id" -> identity.clusterId,
          "topic_id" -> identity.topicId,
          "topic" -> FeatureReadyCodec.Topic
        ) ++ partitions.flatMap { tp =>
          Vector(
            s"beginning_${tp.partition()}" -> beginning(tp).longValue(),
            s"end_${tp.partition()}" -> end(tp).longValue()
          )
        }
        FeatureReadyFiles.claim(path, FeatureReadyCodec.objectBytes(fields: _*))
        consumer.commitSync(end.map { case (tp, offset) =>
          tp -> new OffsetAndMetadata(offset.longValue())
        }.asJava)
        boundary(path, identity, group)
      }
    } finally consumer.close()
  }

  /** Verify one complete batch against a preceding durable boundary; never scan older batches. */
  def accept(
      bootstrap: String,
      identity: FeatureReadyTopicIdentity,
      batch: FeatureReadyBatch,
      journalRoot: Path,
      receiptRoot: Path,
      precedingBoundary: Path,
      acceptancePath: Path,
      maximum: Int,
      failAfterReceiptBeforeCommitAt: Int = -1,
      group: String = Group
  ): Unit = {
    requireOperationalGroup(group)
    val start = boundary(precedingBoundary, identity, group)
    val journal = new FeatureReadyPublicationJournal(batch, journalRoot, identity)
    require(
      journal.acknowledged.keySet == batch.byReadyId.keySet,
      "Publication acknowledgements are incomplete"
    )
    val store = new FeatureReadyReceiptStore(batch, receiptRoot, group, identity)
    verifyRetainedBoundary(bootstrap, batch.topic, start, group)
    if (!java.nio.file.Files.exists(acceptancePath)) {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
        FeatureReadyKafkaConfig.consumer(bootstrap, group)
      )
      val report = new FeatureReadyKafkaConsumer(consumer, batch, store).consume(
        maximum,
        failAfterReceiptBeforeCommitAt = failAfterReceiptBeforeCommitAt,
        minimumStart = Some(start)
      )
      require(report.committedNext == report.fencedEnd, "Incomplete operational acceptance")
      seal(batch, identity, journal, store, start, report.fencedEnd, acceptancePath, group)
      verifyCommittedBoundary(bootstrap, batch.topic, report.fencedEnd, group)
    } else {
      val end = boundary(acceptancePath, identity, group)
      val node = FeatureReadyCodec.parseJson(FeatureReadyFiles.read(acceptancePath))
      require(
        FeatureReadyCodec.text(node, "publication_batch_id") == batch.batchId &&
          offsets(node, "start") == start,
        "Existing operational acceptance conflicts"
      )
      verifyEvidence(batch, journal, store, start, end)
      verifyCommittedBoundary(bootstrap, batch.topic, end, group)
    }
  }

  private def verifyRetainedBoundary(
      bootstrap: String,
      topic: String,
      start: Map[Int, Long],
      group: String
  ): Unit = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      FeatureReadyKafkaConfig.consumer(bootstrap, group)
    )
    try {
      consumer.assign(partitions.asJava)
      val beginning = consumer.beginningOffsets(partitions.asJava).asScala.toMap
      require(
        partitions.forall(tp =>
          tp.topic() == topic && beginning(tp).longValue() <= start(tp.partition())
        ),
        "Kafka retention gap crossed operational batch starting fence"
      )
    } finally consumer.close()
  }

  private def verifyCommittedBoundary(
      bootstrap: String,
      topic: String,
      end: Map[Int, Long],
      group: String
  ): Unit = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      FeatureReadyKafkaConfig.consumer(bootstrap, group)
    )
    try {
      consumer.assign(partitions.asJava)
      val committed = consumer.committed(partitions.toSet.asJava).asScala.toMap
      require(
        partitions.forall(tp =>
          tp.topic() == topic && Option(committed(tp)).exists(_.offset() == end(tp.partition()))
        ),
        "Operational group committed offsets differ from accepted batch boundary"
      )
    } finally consumer.close()
  }

  def observation(root: Path, stage: String, batchId: String, elapsedNanos: Long): Path = {
    require(elapsedNanos >= 0, "Negative local monotonic elapsed time")
    val path = root.resolve(s"${UUID.randomUUID()}.json")
    FeatureReadyFiles.claim(
      path,
      FeatureReadyCodec.objectBytes(
        "observation_contract_version" -> "incremental-shadow-timing-v1",
        "stage" -> stage,
        "publication_batch_id" -> batchId,
        "elapsed_nanos" -> elapsedNanos,
        "observed_at_utc" -> Instant.now().toString
      )
    )
    path
  }

  private def verifyEvidence(
      batch: FeatureReadyBatch,
      journal: FeatureReadyPublicationJournal,
      store: FeatureReadyReceiptStore,
      start: Map[Int, Long],
      end: Map[Int, Long]
  ): Int = {
    val ids = batch.byReadyId.keySet
    val observed = store.observedCoordinates
    require(
      store.receiptIds == ids && journal.acknowledged.keySet == ids,
      "Batch identity incomplete"
    )
    val expectedCoordinates = (0 until 3).flatMap { p =>
      (start(p) until end(p)).map(p -> _)
    }.toSet
    require(
      observed.keySet == expectedCoordinates && observed.values.toSet == ids,
      "Receipts do not cover exact sequential Kafka offset range"
    )
    journal.acknowledged.foreach { case (id, ack) =>
      require(observed.get(ack.partition -> ack.offset).contains(id), "Ack absent from receipts")
    }
    expectedCoordinates.size
  }

  private def seal(
      batch: FeatureReadyBatch,
      identity: FeatureReadyTopicIdentity,
      journal: FeatureReadyPublicationJournal,
      store: FeatureReadyReceiptStore,
      start: Map[Int, Long],
      end: Map[Int, Long],
      path: Path,
      group: String
  ): Unit = {
    val physical = verifyEvidence(batch, journal, store, start, end)
    val fields = Vector[(String, Any)](
      "contract_version" -> AcceptanceVersion,
      "group_id" -> group,
      "cluster_id" -> identity.clusterId,
      "topic_id" -> identity.topicId,
      "topic" -> batch.topic,
      "publication_batch_id" -> batch.batchId,
      "outbox_id" -> batch.outboxId,
      "ready_event_count" -> batch.records.size,
      "physical_record_count" -> physical
    ) ++ (0 until 3).flatMap { p =>
      Vector(s"start_$p" -> start(p), s"end_$p" -> end(p))
    }
    FeatureReadyFiles.claim(path, FeatureReadyCodec.objectBytes(fields: _*))
  }
}
