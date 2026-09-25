package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.Dataset

import io.sentinelaegisforge.streaming.kafka.ingestion.ValidatedTransactionRecord

final case class DeltaBatchWritePlan(
    deltaPath: String,
    txnAppId: String,
    txnVersion: Long
) {
  val options: Map[String, String] = Map(
    "txnAppId" -> txnAppId,
    "txnVersion" -> txnVersion.toString
  )
}

trait DeltaPostCommitHook extends Serializable {
  def afterSuccessfulCommit(batchId: Long): Unit
}

object DeltaPostCommitHook {
  case object NoOp extends DeltaPostCommitHook {
    override def afterSuccessfulCommit(batchId: Long): Unit = ()
  }

  final case class FailAfterBatch(batchIdToFail: Long) extends DeltaPostCommitHook {
    override def afterSuccessfulCommit(batchId: Long): Unit =
      if (batchId == batchIdToFail) {
        throw new IntentionalDeltaPostCommitFailure(batchId)
      }
  }
}

final class IntentionalDeltaPostCommitFailure(batchId: Long)
    extends RuntimeException(
      s"Intentional diagnostic failure after Delta committed batch $batchId"
    )

final class DeltaTransactionSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  require(deltaPath.trim.nonEmpty, "deltaPath must not be blank")
  require(txnAppId.trim.nonEmpty, "txnAppId must not be blank")

  def plan(batchId: Long): DeltaBatchWritePlan = {
    require(batchId >= 0L, "batchId must be non-negative")
    DeltaBatchWritePlan(deltaPath, txnAppId, batchId)
  }

  def writeBatch(batch: Dataset[ValidatedTransactionRecord], batchId: Long): Unit = {
    val writePlan = plan(batchId)
    val storageFrame = ValidatedTransactionDeltaSchema.project(batch)

    if (!storageFrame.isEmpty) {
      storageFrame.write
        .format("delta")
        .mode("append")
        .options(writePlan.options)
        .save(writePlan.deltaPath)

      println(
        s"Delta batch write returned successfully: batchId=$batchId txnAppId=$txnAppId path=$deltaPath"
      )
      postCommitHook.afterSuccessfulCommit(batchId)
    } else {
      println(s"Delta batch skipped because it was empty: batchId=$batchId")
    }
  }
}
