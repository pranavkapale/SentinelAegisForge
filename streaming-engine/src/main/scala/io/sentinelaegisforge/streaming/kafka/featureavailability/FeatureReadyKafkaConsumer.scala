package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

final case class FeatureReadyConsumeReport(
    examined: Int,
    beginning: Map[Int, Long],
    fencedEnd: Map[Int, Long],
    committedNext: Map[Int, Long],
    receipts: FeatureReadyReceiptSummary
)

/** One local research-group member; record-level synchronous commits follow receipt persistence. */
final class FeatureReadyKafkaConsumer(
    consumer: Consumer[Array[Byte], Array[Byte]],
    batch: FeatureReadyBatch,
    store: FeatureReadyReceiptStore
) {
  def consume(
      maximum: Int,
      failBeforeReceiptAt: Int = -1,
      failAfterReceiptBeforeCommitAt: Int = -1
  ): FeatureReadyConsumeReport = {
    require(maximum >= 1 && maximum <= 5000, "Invalid bounded consume maximum")
    consumer.subscribe(Seq(batch.topic).asJava)
    try {
      var first = consumer.poll(Duration.ofMillis(500))
      var waits = 0
      while (consumer.assignment().isEmpty && waits < 60) {
        first = consumer.poll(Duration.ofMillis(500))
        waits += 1
      }
      val partitions = consumer.assignment().asScala.toSet
      require(
        partitions.size == 3 && partitions.forall(_.topic() == batch.topic),
        "Research group did not acquire all three topic partitions"
      )
      val beginning = consumer.beginningOffsets(partitions.asJava).asScala.toMap.map {
        case (tp, offset) => tp -> offset.longValue()
      }
      val fence = consumer.endOffsets(partitions.asJava).asScala.toMap.map { case (tp, offset) =>
        tp -> offset.longValue()
      }
      val committed = consumer.committed(partitions.asJava).asScala.toMap
      val progress = scala.collection.mutable.Map.from(partitions.map { tp =>
        val start = committed.get(tp).flatMap(Option(_)).map(_.offset()).getOrElse(beginning(tp))
        require(
          start >= beginning(tp) && start <= fence(tp),
          "Committed offset outside retained range"
        )
        tp -> start
      })
      var examined = 0
      var idle = 0
      var records = first
      while (progress.exists { case (tp, next) => next < fence(tp) }) {
        if (records.isEmpty) {
          idle += 1
          require(idle < 60, "Consumer incomplete: fenced records not delivered")
        } else idle = 0
        records.iterator().asScala.foreach { record =>
          val tp = new TopicPartition(record.topic(), record.partition())
          if (record.offset() < fence(tp)) {
            require(examined < maximum, "Consumer incomplete: configured maximum reached")
            process(
              record,
              progress(tp),
              examined + 1,
              failBeforeReceiptAt,
              failAfterReceiptBeforeCommitAt
            )
            progress(tp) = record.offset() + 1
            examined += 1
          }
        }
        if (progress.exists { case (tp, next) => next < fence(tp) })
          records = consumer.poll(Duration.ofMillis(500))
      }
      FeatureReadyConsumeReport(
        examined,
        beginning.map { case (tp, offset) => tp.partition() -> offset },
        fence.map { case (tp, offset) => tp.partition() -> offset },
        progress.toMap.map { case (tp, offset) => tp.partition() -> offset },
        store.inspect()
      )
    } finally consumer.close()
  }

  private def process(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      expectedOffset: Long,
      number: Int,
      failBeforeReceiptAt: Int,
      failAfterReceiptBeforeCommitAt: Int
  ): Unit = {
    require(record.offset() == expectedOffset, "Per-partition offset hole or replay within poll")
    store.accept(record, failBeforeReceipt = number == failBeforeReceiptAt)
    if (number == failAfterReceiptBeforeCommitAt)
      throw new IllegalStateException("Injected crash after receipt before offset commit")
    val tp = new TopicPartition(record.topic(), record.partition())
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(record.offset() + 1)).asJava)
  }
}
