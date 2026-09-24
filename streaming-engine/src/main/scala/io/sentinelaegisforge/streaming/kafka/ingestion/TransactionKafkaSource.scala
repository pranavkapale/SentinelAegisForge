package io.sentinelaegisforge.streaming.kafka.ingestion

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

object TransactionKafkaSource {
  def read(
      spark: SparkSession,
      config: TransactionIngestionConfig
  ): Dataset[KafkaSourceRecord] = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.topic)
      .option("startingOffsets", config.startingOffsets)
      .option("failOnDataLoss", "true")
      .load()
      .select(
        col("key"),
        col("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )
      .as[KafkaSourceRecord]
  }
}
