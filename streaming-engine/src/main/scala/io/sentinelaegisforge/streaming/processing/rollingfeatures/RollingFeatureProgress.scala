package io.sentinelaegisforge.streaming.processing.rollingfeatures

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.streaming.StreamingQueryProgress

final case class RollingFeatureStateProgress(
    operatorName: String,
    numRowsTotal: Long,
    numRowsUpdated: Long,
    numRowsRemoved: Long,
    memoryUsedBytes: Long,
    numStateStoreInstances: Long,
    customMetrics: Map[String, Long]
)

final case class RollingFeatureProgress(
    batchId: Long,
    watermark: Option[String],
    inputRows: Long,
    stateOperators: Vector[RollingFeatureStateProgress]
)

object RollingFeatureProgress {
  def from(progress: StreamingQueryProgress): RollingFeatureProgress =
    RollingFeatureProgress(
      batchId = progress.batchId,
      watermark = Option(progress.eventTime)
        .flatMap(_.asScala.get("watermark"))
        .filter(_.nonEmpty),
      inputRows = progress.numInputRows,
      stateOperators = Option(progress.stateOperators)
        .map(_.toVector.map { operator =>
          RollingFeatureStateProgress(
            operatorName = operator.operatorName,
            numRowsTotal = operator.numRowsTotal,
            numRowsUpdated = operator.numRowsUpdated,
            numRowsRemoved = operator.numRowsRemoved,
            memoryUsedBytes = operator.memoryUsedBytes,
            numStateStoreInstances = operator.numStateStoreInstances,
            customMetrics = Option(operator.customMetrics)
              .map(
                _.asScala.iterator
                  .collect {
                    case (name, value) if TimerMetrics.contains(name) => name -> value.longValue()
                  }
                  .toMap
              )
              .getOrElse(Map.empty)
          )
        })
        .getOrElse(Vector.empty)
    )

  private val TimerMetrics = Set("numRegisteredTimers", "numDeletedTimers", "numExpiredTimers")
}

object RollingFeatureProgressReporter {
  def report(progress: Seq[RollingFeatureProgress]): Unit =
    if (progress.isEmpty) println("Rolling feature progress: no micro-batch executed")
    else
      progress.foreach { batch =>
        if (batch.stateOperators.isEmpty) {
          println(
            s"Rolling feature progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} stateOperator=unavailable"
          )
        } else
          batch.stateOperators.foreach { state =>
            println(
              s"Rolling feature progress: batchId=${batch.batchId} watermark=${batch.watermark.getOrElse("unavailable")} inputRows=${batch.inputRows} stateOperator=${state.operatorName} numRowsTotal=${state.numRowsTotal} numRowsUpdated=${state.numRowsUpdated} numRowsRemoved=${state.numRowsRemoved} memoryUsedBytes=${state.memoryUsedBytes} numStateStoreInstances=${state.numStateStoreInstances} customMetrics=${state.customMetrics.toVector.sorted.mkString("{", ",", "}")}"
            )
          }
      }
}
