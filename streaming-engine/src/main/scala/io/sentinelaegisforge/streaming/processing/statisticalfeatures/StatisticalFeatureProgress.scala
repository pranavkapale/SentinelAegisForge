package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.streaming.StreamingQueryProgress

object StatisticalFeatureProgress {
  def report(progress: Seq[StreamingQueryProgress]): Unit = {
    if (progress.isEmpty) println("Statistical feature progress: no micro-batch executed")
    progress.foreach { batch =>
      val watermark = Option(batch.eventTime)
        .flatMap(_.asScala.get("watermark"))
        .getOrElse("unavailable")
      Option(batch.stateOperators).toVector.flatMap(_.toVector).foreach { state =>
        val timerMetrics = Option(state.customMetrics)
          .map(
            _.asScala.iterator
              .collect {
                case (name, value)
                    if Set("numRegisteredTimers", "numDeletedTimers", "numExpiredTimers")
                      .contains(name) =>
                  name -> value.longValue()
              }
              .toVector
              .sorted
          )
          .getOrElse(Vector.empty)
        println(
          s"Statistical feature progress: batchId=${batch.batchId} watermark=$watermark inputRows=${batch.numInputRows} stateOperator=${state.operatorName} numRowsTotal=${state.numRowsTotal} numRowsUpdated=${state.numRowsUpdated} numRowsRemoved=${state.numRowsRemoved} memoryUsedBytes=${state.memoryUsedBytes} numStateStoreInstances=${state.numStateStoreInstances} timerMetrics=${timerMetrics.mkString("{", ",", "}")}"
        )
      }
    }
  }
}
