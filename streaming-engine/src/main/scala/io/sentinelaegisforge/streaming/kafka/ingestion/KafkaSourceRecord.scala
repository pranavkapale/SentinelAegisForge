package io.sentinelaegisforge.streaming.kafka.ingestion

import java.sql.Timestamp

/** Raw transport data exposed by Spark's Kafka source. */
final case class KafkaSourceRecord(
    key: Array[Byte],
    value: Array[Byte],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Timestamp
) {
  def coordinates: KafkaRecordCoordinates = KafkaRecordCoordinates(topic, partition, offset)
}

final case class KafkaRecordCoordinates(topic: String, partition: Int, offset: Long) {
  override def toString: String = s"$topic-$partition@$offset"
}
