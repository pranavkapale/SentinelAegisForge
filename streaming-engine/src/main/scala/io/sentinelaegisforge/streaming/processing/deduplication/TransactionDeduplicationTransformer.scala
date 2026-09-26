package io.sentinelaegisforge.streaming.processing.deduplication

import org.apache.spark.sql.DataFrame

import io.sentinelaegisforge.streaming.persistence.delta.ValidatedTransactionDeltaSchema

object TransactionDeduplicationTransformer {
  val EventTimeColumn: String = "event_time"
  val DeduplicationKey: String = "event_id"

  def deduplicate(source: DataFrame, watermarkDelay: String): DataFrame = {
    require(watermarkDelay.trim.nonEmpty, "watermarkDelay must not be blank")
    requireExpectedStorageSchema(source)

    source
      .withWatermark(EventTimeColumn, watermarkDelay)
      .dropDuplicatesWithinWatermark(DeduplicationKey)
  }

  private def requireExpectedStorageSchema(frame: DataFrame): Unit = {
    val actual = frame.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(
      actual == ValidatedTransactionDeltaSchema.columns,
      s"Unexpected validated-transaction source schema: $actual"
    )
  }
}
