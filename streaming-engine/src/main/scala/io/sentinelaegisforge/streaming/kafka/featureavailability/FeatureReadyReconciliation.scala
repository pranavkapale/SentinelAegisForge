package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

final case class FeatureReadyReconciliation(
    sourceUnique: Int,
    journalAcknowledgedUnique: Int,
    brokerPhysical: Int,
    brokerDuplicatePhysical: Int,
    consumerUniqueReceipts: Int,
    consumerDuplicateDeliveries: Int,
    brokerByPartition: Map[Int, Int],
    brokerBeginning: Map[Int, Long],
    brokerEnd: Map[Int, Long],
    groupCommittedNext: Map[Int, Long]
)

object FeatureReadyReconciliation {
  def verify(
      batch: FeatureReadyBatch,
      journal: FeatureReadyPublicationJournal,
      store: FeatureReadyReceiptStore,
      bootstrap: String,
      group: String
  ): FeatureReadyReconciliation = {
    val identity = batch.byReadyId.keySet
    val acknowledged = journal.acknowledged
    val receiptIds = store.receiptIds
    require(
      identity == acknowledged.keySet && identity == receiptIds,
      "Source/acknowledgement/receipt identity reconciliation incomplete"
    )
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      FeatureReadyKafkaConfig.consumer(bootstrap, group)
    )
    try {
      val partitions = (0 until 3).map(new TopicPartition(batch.topic, _)).toVector
      consumer.assign(partitions.asJava)
      val beginning = consumer.beginningOffsets(partitions.asJava).asScala.toMap
      val end = consumer.endOffsets(partitions.asJava).asScala.toMap
      partitions.foreach(tp => consumer.seek(tp, beginning(tp)))
      val observed = scala.collection.mutable.Map.empty[(Int, Long), String]
      var idle = 0
      while (partitions.exists(tp => consumer.position(tp) < end(tp))) {
        val records = consumer.poll(Duration.ofMillis(500))
        if (records.isEmpty) {
          idle += 1
          require(idle < 60, "Broker scan incomplete")
        } else idle = 0
        records.iterator().asScala.foreach { record =>
          val tp = new TopicPartition(record.topic(), record.partition())
          if (record.offset() < end(tp)) {
            val event = FeatureReadyCodec.validateRecord(record.key(), record.value())
            val expected = batch.byReadyId.getOrElse(
              event.readyEventId,
              throw new IllegalArgumentException("Unknown broker ready-event ID")
            )
            require(
              java.util.Arrays.equals(record.value(), expected.bytes),
              "Broker envelope differs from verified outbox"
            )
            require(observed.size < 20000, "Broker scan exceeds bounded research limit")
            val coordinate = record.partition() -> record.offset()
            require(!observed.contains(coordinate), "Duplicate broker coordinate")
            observed(coordinate) = event.readyEventId
          }
        }
      }
      require(observed.values.toSet == identity, "Broker/source identity set mismatch")
      acknowledged.foreach { case (id, ack) =>
        require(
          observed.get(ack.partition -> ack.offset).contains(id),
          "Journalled acknowledgement absent from broker"
        )
      }
      store.receiptCoordinates.foreach { case (id, coordinates) =>
        require(observed.get(coordinates).contains(id), "Receipt's first broker coordinate missing")
      }
      require(
        store.observedCoordinates == observed.toMap,
        "Consumer observations do not cover declared broker range"
      )
      val committed = consumer.committed(partitions.toSet.asJava).asScala.toMap
      require(
        partitions.forall(tp => committed.get(tp).flatMap(Option(_)).exists(_.offset() >= end(tp))),
        "Consumer group has not committed the declared broker range"
      )
      val summary = store.inspect()
      FeatureReadyReconciliation(
        identity.size,
        acknowledged.size,
        observed.size,
        observed.size - identity.size,
        summary.uniqueReceipts,
        summary.duplicateObservations,
        observed.keys.groupBy(_._1).view.mapValues(_.size).toMap,
        beginning.map { case (tp, offset) => tp.partition() -> offset },
        end.map { case (tp, offset) => tp.partition() -> offset },
        committed.map { case (tp, offset) => tp.partition() -> offset.offset() }
      )
    } finally consumer.close()
  }
}
