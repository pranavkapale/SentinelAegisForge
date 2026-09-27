package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}

import io.sentinelaegisforge.streaming.processing.customerstate.{
  CustomerActivityProcessor,
  CustomerActivitySnapshot
}

object CustomerActivityDeltaSchema {
  val columns = Vector(
    "customer_id" -> StringType,
    "lifecycle_type" -> StringType,
    "batch_event_count" -> LongType,
    "batch_amount_total" -> DecimalType(
      CustomerActivityProcessor.AmountPrecision,
      CustomerActivityProcessor.AmountScale
    ),
    "state_event_count" -> LongType,
    "state_amount_total" -> DecimalType(
      CustomerActivityProcessor.AmountPrecision,
      CustomerActivityProcessor.AmountScale
    ),
    "state_first_event_time" -> TimestampType,
    "state_latest_event_time" -> TimestampType,
    "state_expiry_time" -> TimestampType
  )

  def project(snapshots: Dataset[CustomerActivitySnapshot]): DataFrame = {
    val amountType = DecimalType(
      CustomerActivityProcessor.AmountPrecision,
      CustomerActivityProcessor.AmountScale
    )
    val projected = snapshots.select(
      col("customerId").cast(StringType).as("customer_id"),
      col("lifecycleType").cast(StringType).as("lifecycle_type"),
      col("batchEventCount").cast(LongType).as("batch_event_count"),
      col("batchAmountTotal").cast(amountType).as("batch_amount_total"),
      col("stateEventCount").cast(LongType).as("state_event_count"),
      col("stateAmountTotal").cast(amountType).as("state_amount_total"),
      col("stateFirstEventTime").cast(TimestampType).as("state_first_event_time"),
      col("stateLatestEventTime").cast(TimestampType).as("state_latest_event_time"),
      col("stateExpiryTime").cast(TimestampType).as("state_expiry_time")
    )
    requireExpectedSchema(projected)
    projected
  }

  private[delta] def requireExpectedSchema(frame: DataFrame): Unit = {
    val actual = frame.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(actual == columns, s"Unexpected customer-activity snapshot schema: $actual")
  }
}

final class CustomerActivityDeltaSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  private val delegate = new DeltaTransactionSink(deltaPath, txnAppId, postCommitHook)

  def plan(batchId: Long): DeltaBatchWritePlan = delegate.plan(batchId)

  def writeBatch(batch: Dataset[CustomerActivitySnapshot], batchId: Long): Unit = {
    val storageFrame = CustomerActivityDeltaSchema.project(batch).persist()
    try {
      if (storageFrame.isEmpty) {
        println(s"Customer activity Delta batch skipped because it was empty: batchId=$batchId")
      } else {
        delegate.writeNonEmptyStorageBatch(storageFrame, batchId)
      }
    } finally {
      storageFrame.unpersist()
    }
  }
}
