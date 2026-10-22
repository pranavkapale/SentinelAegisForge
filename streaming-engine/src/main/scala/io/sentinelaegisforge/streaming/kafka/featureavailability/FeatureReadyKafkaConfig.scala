package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

final case class FeatureReadyTopicIdentity(clusterId: String, topicId: String, partitions: Int)

object FeatureReadyKafkaConfig {
  def producer(bootstrap: String): Properties = {
    require(bootstrap.trim.nonEmpty, "bootstrap server required")
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    p.put(ProducerConfig.RETRIES_CONFIG, "5")
    p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000")
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000")
    p
  }

  def consumer(bootstrap: String, group: String): Properties = {
    require(bootstrap.trim.nonEmpty && group.trim.nonEmpty, "bootstrap and stable group required")
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
    p
  }

  def topic(bootstrap: String): FeatureReadyTopicIdentity = {
    val p = new Properties()
    p.put("bootstrap.servers", bootstrap)
    p.put("request.timeout.ms", "5000")
    p.put("default.api.timeout.ms", "10000")
    val admin = AdminClient.create(p)
    try {
      val description = admin
        .describeTopics(Seq(FeatureReadyCodec.Topic).asJava)
        .topicNameValues()
        .get(FeatureReadyCodec.Topic)
        .get()
      require(description.partitions().size() == 3, "features.ready must have three partitions")
      require(
        description.partitions().asScala.forall(_.replicas().size() == 1),
        "features.ready must have replication factor one"
      )
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, FeatureReadyCodec.Topic)
      val config = admin.describeConfigs(Seq(resource).asJava).all().get().get(resource)
      require(
        config.get("cleanup.policy") != null &&
          config.get("cleanup.policy").value() == "delete",
        "features.ready must use delete retention"
      )
      val topicId = description.topicId().toString
      require(topicId != null && topicId.nonEmpty, "Kafka topic ID unavailable")
      FeatureReadyTopicIdentity(admin.describeCluster().clusterId().get(), topicId, 3)
    } finally admin.close()
  }
}
