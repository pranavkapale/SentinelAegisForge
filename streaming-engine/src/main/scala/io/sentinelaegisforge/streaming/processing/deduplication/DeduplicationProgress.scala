package io.sentinelaegisforge.streaming.processing.deduplication

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.streaming.{StateOperatorProgress, StreamingQueryProgress}

final case class DeduplicationStateProgress(
    operatorName: String,
    numRowsTotal: Long,
    numRowsUpdated: Long,
    numRowsRemoved: Long,
    numRowsDroppedByWatermark: Long,
    memoryUsedBytes: Long
)

final case class DeduplicationProgress(
    batchId: Long,
    watermark: Option[String],
    inputRows: Long,
    outputRows: Option[Long],
    stateOperators: Vector[DeduplicationStateProgress]
) {
  def rowsDroppedByWatermark: Long =
    stateOperators.map(_.numRowsDroppedByWatermark).sum
}

object DeduplicationProgress {
  def from(progress: StreamingQueryProgress): DeduplicationProgress = {
    val eventTime = Option(progress.eventTime).map(_.asScala.toMap).getOrElse(Map.empty)
    val sinkOutputRows = Option(progress.sink)
      .map(_.numOutputRows)
      .filter(_ >= 0L)

    DeduplicationProgress(
      batchId = progress.batchId,
      watermark = eventTime.get("watermark").filter(_.nonEmpty),
      inputRows = progress.numInputRows,
      outputRows = sinkOutputRows,
      stateOperators = Option(progress.stateOperators)
        .map(_.toVector.map(stateProgress))
        .getOrElse(Vector.empty)
    )
  }

  private def stateProgress(progress: StateOperatorProgress): DeduplicationStateProgress =
    DeduplicationStateProgress(
      operatorName = progress.operatorName,
      numRowsTotal = progress.numRowsTotal,
      numRowsUpdated = progress.numRowsUpdated,
      numRowsRemoved = progress.numRowsRemoved,
      numRowsDroppedByWatermark = progress.numRowsDroppedByWatermark,
      memoryUsedBytes = progress.memoryUsedBytes
    )
}

object DeduplicationProgressReporter {
  def report(progress: Seq[DeduplicationProgress]): Unit = {
    if (progress.isEmpty) {
      println("Deduplication progress: no micro-batch executed")
    } else {
      progress.foreach { batch =>
        val outputRows = batch.outputRows.map(_.toString).getOrElse("unavailable")
        if (batch.stateOperators.isEmpty) {
          println(
            s"Deduplication progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} outputRows=$outputRows stateOperator=unavailable"
          )
        } else {
          batch.stateOperators.foreach { state =>
            println(
              s"Deduplication progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} outputRows=$outputRows stateOperator=${state.operatorName} numRowsTotal=${state.numRowsTotal} numRowsUpdated=${state.numRowsUpdated} numRowsRemoved=${state.numRowsRemoved} numRowsDroppedByWatermark=${state.numRowsDroppedByWatermark} memoryUsedBytes=${state.memoryUsedBytes}"
            )
          }
        }
      }
    }
  }
}
