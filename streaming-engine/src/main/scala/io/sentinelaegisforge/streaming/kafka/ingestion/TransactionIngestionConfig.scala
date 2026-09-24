package io.sentinelaegisforge.streaming.kafka.ingestion

final case class TransactionIngestionConfig(
    bootstrapServers: String,
    schemaRegistryUrl: String,
    topic: String,
    checkpointLocation: String,
    startingOffsets: String,
    sparkMaster: String
) {
  require(bootstrapServers.trim.nonEmpty, "bootstrapServers must not be blank")
  require(schemaRegistryUrl.trim.nonEmpty, "schemaRegistryUrl must not be blank")
  require(topic == TransactionIngestionConfig.DefaultTopic, "only transactions.raw is supported")
  require(checkpointLocation.trim.nonEmpty, "checkpointLocation must not be blank")
  require(startingOffsets.trim.nonEmpty, "startingOffsets must not be blank")
  require(sparkMaster.trim.nonEmpty, "sparkMaster must not be blank")
}

object TransactionIngestionConfig {
  val DefaultBootstrapServers: String = "localhost:9092"
  val DefaultSchemaRegistryUrl: String = "http://localhost:8081"
  val DefaultTopic: String = "transactions.raw"
  val DefaultCheckpointLocation: String = ".local/checkpoints/transaction-ingestion"
  val DefaultStartingOffsets: String = "earliest"
  val DefaultSparkMaster: String = "local[*]"

  val localDefault: TransactionIngestionConfig = TransactionIngestionConfig(
    bootstrapServers = DefaultBootstrapServers,
    schemaRegistryUrl = DefaultSchemaRegistryUrl,
    topic = DefaultTopic,
    checkpointLocation = DefaultCheckpointLocation,
    startingOffsets = DefaultStartingOffsets,
    sparkMaster = DefaultSparkMaster
  )
}
