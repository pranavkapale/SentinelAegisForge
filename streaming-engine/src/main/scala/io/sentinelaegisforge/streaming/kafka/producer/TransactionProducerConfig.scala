package io.sentinelaegisforge.streaming.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.subject.TopicNameStrategy

final case class TransactionProducerConfig(
    bootstrapServers: String,
    schemaRegistryUrl: String,
    topic: String = TransactionProducerConfig.DefaultTopic
) {
  require(bootstrapServers.trim.nonEmpty, "bootstrapServers must not be blank")
  require(schemaRegistryUrl.trim.nonEmpty, "schemaRegistryUrl must not be blank")
  require(topic.trim.nonEmpty, "topic must not be blank")
}

object TransactionProducerConfig {
  val DefaultTopic: String = "transactions.raw"

  def properties(config: TransactionProducerConfig): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer].getName
    )
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put("schema.registry.url", config.schemaRegistryUrl)
    properties.put("auto.register.schemas", "false")
    properties.put("value.subject.name.strategy", classOf[TopicNameStrategy].getName)
    properties
  }
}
