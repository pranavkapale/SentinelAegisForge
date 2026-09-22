package io.sentinelaegisforge.streaming.kafka.producer

import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.funsuite.AnyFunSuite

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.subject.TopicNameStrategy

final class TransactionProducerConfigSpec extends AnyFunSuite {
  test("producer properties enforce registry-backed reliable local delivery") {
    val properties = TransactionProducerConfig.properties(
      TransactionProducerConfig(
        bootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081"
      )
    )

    assert(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == "localhost:9092")
    assert(
      properties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) ==
        "org.apache.kafka.common.serialization.StringSerializer"
    )
    assert(
      properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) ==
        classOf[KafkaAvroSerializer].getName
    )
    assert(properties.getProperty(ProducerConfig.ACKS_CONFIG) == "all")
    assert(properties.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG) == "true")
    assert(properties.getProperty("schema.registry.url") == "http://localhost:8081")
    assert(properties.getProperty("auto.register.schemas") == "false")
    assert(
      properties.getProperty("value.subject.name.strategy") == classOf[TopicNameStrategy].getName
    )
    assert(!properties.containsKey("use.latest.version"))
    assert(!properties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
  }
}
