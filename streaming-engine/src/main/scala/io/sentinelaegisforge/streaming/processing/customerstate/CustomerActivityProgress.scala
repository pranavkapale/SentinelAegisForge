package io.sentinelaegisforge.streaming.processing.customerstate

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.streaming.{StateOperatorProgress, StreamingQueryProgress}

final case class CustomerActivityStateProgress(
    operatorName: String,
    numRowsTotal: Long,
    numRowsUpdated: Long,
    numRowsRemoved: Long,
    memoryUsedBytes: Long,
    numShufflePartitions: Long,
    numStateStoreInstances: Long,
    customMetrics: Map[String, Long]
)

final case class CustomerActivityProgress(
    batchId: Long,
    watermark: Option[String],
    inputRows: Long,
    outputRows: Option[Long],
    stateOperators: Vector[CustomerActivityStateProgress]
)

object CustomerActivityProgress {
  def from(progress: StreamingQueryProgress): CustomerActivityProgress = {
    val eventTime = Option(progress.eventTime).map(_.asScala.toMap).getOrElse(Map.empty)
    CustomerActivityProgress(
      batchId = progress.batchId,
      watermark = eventTime.get("watermark").filter(_.nonEmpty),
      inputRows = progress.numInputRows,
      outputRows = Option(progress.sink).map(_.numOutputRows).filter(_ >= 0L),
      stateOperators = Option(progress.stateOperators)
        .map(_.toVector.map(stateProgress))
        .getOrElse(Vector.empty)
    )
  }

  private def stateProgress(progress: StateOperatorProgress): CustomerActivityStateProgress =
    CustomerActivityStateProgress(
      operatorName = progress.operatorName,
      numRowsTotal = progress.numRowsTotal,
      numRowsUpdated = progress.numRowsUpdated,
      numRowsRemoved = progress.numRowsRemoved,
      memoryUsedBytes = progress.memoryUsedBytes,
      numShufflePartitions = progress.numShufflePartitions,
      numStateStoreInstances = progress.numStateStoreInstances,
      customMetrics = Option(progress.customMetrics)
        .map(_.asScala.iterator.map { case (name, value) => name -> value.longValue() }.toMap)
        .getOrElse(Map.empty)
    )
}

object CustomerActivityProgressReporter {
  def report(progress: Seq[CustomerActivityProgress]): Unit = {
    if (progress.isEmpty) {
      println("Customer activity progress: no micro-batch executed")
    } else {
      progress.foreach { batch =>
        val outputRows = batch.outputRows.map(_.toString).getOrElse("unavailable")
        if (batch.stateOperators.isEmpty) {
          println(
            s"Customer activity progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} outputRows=$outputRows stateOperator=unavailable"
          )
        } else {
          batch.stateOperators.foreach { state =>
            val custom = state.customMetrics.toVector.sorted.mkString("{", ",", "}")
            println(
              s"Customer activity progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} outputRows=$outputRows stateOperator=${state.operatorName} numRowsTotal=${state.numRowsTotal} numRowsUpdated=${state.numRowsUpdated} numRowsRemoved=${state.numRowsRemoved} memoryUsedBytes=${state.memoryUsedBytes} numShufflePartitions=${state.numShufflePartitions} numStateStoreInstances=${state.numStateStoreInstances} customMetrics=$custom"
            )
          }
        }
      }
    }
  }
}
