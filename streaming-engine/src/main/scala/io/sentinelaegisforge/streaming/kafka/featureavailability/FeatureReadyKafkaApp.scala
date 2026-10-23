package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.file.Path

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

/** Explicit bounded local research transport, never model inference or payment control. */
object FeatureReadyKafkaApp {
  def main(args: Array[String]): Unit = {
    val options = args
      .filter(item => item.startsWith("--") && item.contains("="))
      .map { item =>
        val parts = item.drop(2).split("=", 2)
        parts(0) -> parts(1)
      }
      .toMap
    def required(name: String): String =
      options.getOrElse(name, throw new IllegalArgumentException(s"Missing --$name"))
    val mode = required("mode")
    val bootstrap = required("bootstrap-servers")
    val topicIdentity = FeatureReadyKafkaConfig.topic(bootstrap)
    if (mode == "topic") {
      println(
        s"topic=${FeatureReadyCodec.Topic} cluster_id=${topicIdentity.clusterId} " +
          s"topic_id=${topicIdentity.topicId} partitions=${topicIdentity.partitions} " +
          "replication_factor=1 cleanup_policy=delete"
      )
      return
    }
    val batch = FeatureReadyBatch.load(Path.of(required("batch")))
    val group = options.getOrElse("group", "sentinel-feature-ready-research-v1")
    mode match {
      case "publish" =>
        val journal = new FeatureReadyPublicationJournal(
          batch,
          Path.of(required("journal-root")),
          topicIdentity
        )
        val producer =
          new KafkaProducer[Array[Byte], Array[Byte]](FeatureReadyKafkaConfig.producer(bootstrap))
        val publisher = new FeatureReadyKafkaPublisher(producer, batch, journal)
        val delivered = publisher.publish(
          args.contains("--replay-acknowledged"),
          options.get("fail-after-ack-before-journal-at").map(_.toInt).getOrElse(-1),
          options.get("maximum-deliveries").map(_.toInt).getOrElse(5000)
        )
        println(s"delivered_this_attempt=$delivered ${journal.summary}")
      case "inspect-publication" =>
        val journal = new FeatureReadyPublicationJournal(
          batch,
          Path.of(required("journal-root")),
          topicIdentity
        )
        println(journal.summary)
      case "consume" =>
        val store = new FeatureReadyReceiptStore(
          batch,
          Path.of(required("receipt-root")),
          group,
          topicIdentity
        )
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
          FeatureReadyKafkaConfig.consumer(bootstrap, group)
        )
        val report = new FeatureReadyKafkaConsumer(consumer, batch, store).consume(
          required("maximum").toInt,
          options.get("fail-before-receipt-at").map(_.toInt).getOrElse(-1),
          options.get("fail-after-receipt-before-commit-at").map(_.toInt).getOrElse(-1)
        )
        println(report)
      case "inspect-receipts" =>
        val store = new FeatureReadyReceiptStore(
          batch,
          Path.of(required("receipt-root")),
          group,
          topicIdentity
        )
        println(store.inspect())
      case "reconcile" =>
        val journal = new FeatureReadyPublicationJournal(
          batch,
          Path.of(required("journal-root")),
          topicIdentity
        )
        val store = new FeatureReadyReceiptStore(
          batch,
          Path.of(required("receipt-root")),
          group,
          topicIdentity
        )
        println(FeatureReadyReconciliation.verify(batch, journal, store, bootstrap, group))
      case "seal-shadow-inbox" =>
        require(group == FeatureReadyShadowInbox.Group, "Shadow inbox requires its stable group")
        val receiptRoot = Path.of(required("receipt-root"))
        val journal = new FeatureReadyPublicationJournal(
          batch,
          Path.of(required("journal-root")),
          topicIdentity
        )
        val store = new FeatureReadyReceiptStore(batch, receiptRoot, group, topicIdentity)
        val reconciliation = FeatureReadyReconciliation.verify(
          batch,
          journal,
          store,
          bootstrap,
          group
        )
        println(
          FeatureReadyShadowInbox.sealReconciled(
            batch,
            store,
            receiptRoot,
            topicIdentity,
            reconciliation,
            Path.of(required("inbox-root"))
          )
        )
      case "offsets" =>
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
          FeatureReadyKafkaConfig.consumer(bootstrap, s"$group-offset-inspection")
        )
        try {
          val partitions = (0 until topicIdentity.partitions)
            .map(partition => new TopicPartition(batch.topic, partition))
            .toVector
          consumer.assign(partitions.asJava)
          val beginning = consumer.beginningOffsets(partitions.asJava).asScala.toMap
          val end = consumer.endOffsets(partitions.asJava).asScala.toMap
          println(s"topic_id=${topicIdentity.topicId} beginning=${beginning.map {
              case (tp, offset) => tp.partition() -> offset
            }} end=${end.map { case (tp, offset) => tp.partition() -> offset }}")
        } finally consumer.close()
      case "initialize-fault-group-at-end" =>
        require(
          group.startsWith("sentinel-feature-ready-fault-"),
          "Only explicitly named isolated fault groups may be initialized at end"
        )
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
          FeatureReadyKafkaConfig.consumer(bootstrap, group)
        )
        try {
          val partitions = (0 until topicIdentity.partitions)
            .map(partition => new TopicPartition(batch.topic, partition))
            .toVector
          consumer.assign(partitions.asJava)
          val committed = consumer.committed(partitions.toSet.asJava).asScala.toMap
          require(committed.values.forall(_ == null), "Fault group already has committed progress")
          val end = consumer.endOffsets(partitions.asJava).asScala.toMap
          consumer.commitSync(end.map { case (tp, offset) =>
            tp -> new OffsetAndMetadata(offset.longValue())
          }.asJava)
          println(s"isolated_fault_group=$group excluded_prior_range_end=${end.map {
              case (tp, offset) => tp.partition() -> offset
            }}")
        } finally consumer.close()
      case other => throw new IllegalArgumentException(s"Unsupported feature-ready mode: $other")
    }
  }
}
