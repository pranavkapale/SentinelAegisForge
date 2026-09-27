package io.sentinelaegisforge.streaming.processing.customerstate

import java.time.Duration

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import io.sentinelaegisforge.streaming.persistence.delta.ValidatedTransactionDeltaSchema

object CustomerActivityTransformer {
  val EventTimeColumn: String = "event_time"
  val CustomerKeyColumn: String = "customer_id"

  def transform(
      source: DataFrame,
      watermarkDelay: String,
      inactivityTimeout: Duration
  ): Dataset[CustomerActivitySnapshot] = {
    require(watermarkDelay.trim.nonEmpty, "watermarkDelay must not be blank")
    require(!inactivityTimeout.isZero && !inactivityTimeout.isNegative)
    requireExpectedStorageSchema(source)

    val input = source
      .select(col(CustomerKeyColumn), col(EventTimeColumn), col("amount"))
      .withWatermark(EventTimeColumn, watermarkDelay)
      .as[CustomerActivityInput](Encoders.product[CustomerActivityInput])

    implicit val snapshotEncoder = Encoders.product[CustomerActivitySnapshot]

    input
      .groupByKey(_.customer_id)(Encoders.STRING)
      .transformWithState(
        new CustomerActivityProcessor(inactivityTimeout),
        TimeMode.EventTime(),
        OutputMode.Update()
      )
  }

  private def requireExpectedStorageSchema(frame: DataFrame): Unit = {
    val actual = frame.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(
      actual == ValidatedTransactionDeltaSchema.columns,
      s"Unexpected deduplicated-transaction source schema: $actual"
    )
  }
}
