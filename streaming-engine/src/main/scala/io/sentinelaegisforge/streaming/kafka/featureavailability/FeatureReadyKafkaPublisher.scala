package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

/** At-least-once across process restarts; acknowledgement journal narrows normal replay. */
final class FeatureReadyKafkaPublisher(
    producer: Producer[Array[Byte], Array[Byte]],
    batch: FeatureReadyBatch,
    journal: FeatureReadyPublicationJournal
) {
  def publish(
      replayAcknowledged: Boolean = false,
      failAfterBrokerAckBeforeJournalAt: Int = -1,
      maximumDeliveries: Int = 5000
  ): Int = {
    require(
      maximumDeliveries >= 1 && maximumDeliveries <= 5000,
      "Invalid bounded publication maximum"
    )
    val durable = journal.acknowledged.keySet
    val pending = (if (replayAcknowledged) batch.records
                   else batch.records.filterNot(record => durable(record.event.readyEventId)))
      .take(maximumDeliveries)
    val attempt = journal.start(replayAcknowledged)
    var delivered = 0
    try {
      pending.foreach { record =>
        val metadata = producer
          .send(new ProducerRecord(batch.topic, FeatureReadyCodec.key(record.event), record.bytes))
          .get(130, TimeUnit.SECONDS)
        delivered += 1
        if (delivered == failAfterBrokerAckBeforeJournalAt)
          throw new IllegalStateException(
            "Injected crash after broker acknowledgement before journal"
          )
        journal.record(attempt, record, metadata.partition(), metadata.offset(), replayAcknowledged)
      }
      producer.flush()
      journal.complete(attempt, delivered, replayAcknowledged)
      delivered
    } finally producer.close()
  }
}
