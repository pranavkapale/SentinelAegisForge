package io.sentinelaegisforge.streaming.kafka.ingestion

import java.nio.file.Path
import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import io.sentinelaegisforge.streaming.kafka.featureavailability.{
  FeatureReadyCodec,
  FeatureReadyFiles,
  FeatureReadyKafkaConfig
}

/** Read-only broker offset fence for one new transactions.raw integration lineage. */
object TransactionTopicFenceApp {
  val ContractVersion = "transaction-topic-starting-fence-v1"
  val Topic = "transactions.raw"

  def main(args: Array[String]): Unit = {
    val options = args.map { argument =>
      require(argument.startsWith("--") && argument.contains("="), s"Invalid argument: $argument")
      val parts = argument.drop(2).split("=", 2)
      parts(0) -> parts(1)
    }.toMap
    require(
      options.keySet == Set("bootstrap-servers", "output-path"),
      "Require --bootstrap-servers and --output-path only"
    )
    val bootstrap = options("bootstrap-servers")
    val adminProperties = new Properties()
    adminProperties.put("bootstrap.servers", bootstrap)
    adminProperties.put("request.timeout.ms", "5000")
    adminProperties.put("default.api.timeout.ms", "10000")
    val admin = AdminClient.create(adminProperties)
    val (clusterId, topicId, partitions) =
      try {
        val description = admin.describeTopics(Seq(Topic).asJava).topicNameValues().get(Topic).get()
        require(description.partitions().size() == 3, "transactions.raw partition count changed")
        require(
          description.partitions().asScala.forall(_.replicas().size() == 1),
          "transactions.raw replication factor changed"
        )
        (
          admin.describeCluster().clusterId().get(),
          description.topicId().toString,
          description
            .partitions()
            .asScala
            .map(info => new TopicPartition(Topic, info.partition()))
            .toVector
        )
      } finally admin.close()
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      FeatureReadyKafkaConfig.consumer(bootstrap, "sentinel-phase28-transaction-fence-inspection")
    )
    val (beginning, end) =
      try {
        consumer.assign(partitions.asJava)
        (
          consumer.beginningOffsets(partitions.asJava).asScala.toMap,
          consumer.endOffsets(partitions.asJava).asScala.toMap
        )
      } finally consumer.close()
    val fields = Vector[(String, Any)](
      "contract_version" -> ContractVersion,
      "cluster_id" -> clusterId,
      "topic_id" -> topicId,
      "topic" -> Topic,
      "partition_count" -> partitions.size
    ) ++ partitions.sortBy(_.partition()).flatMap { partition =>
      Vector(
        s"beginning_${partition.partition()}" -> beginning(partition).longValue(),
        s"end_${partition.partition()}" -> end(partition).longValue()
      )
    }
    val path = Path.of(options("output-path"))
    FeatureReadyFiles.claim(path, FeatureReadyCodec.objectBytes(fields: _*))
    println(
      s"transaction_starting_fence=$path cluster_id=$clusterId topic_id=$topicId end=${end.map {
          case (partition, offset) => partition.partition() -> offset
        }}"
    )
  }
}
