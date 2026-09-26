package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.DataFrame

/** Durable sink for watermark-deduplicated storage records. */
final class DeduplicatedTransactionDeltaSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  private val delegate = new DeltaTransactionSink(deltaPath, txnAppId, postCommitHook)

  def plan(batchId: Long): DeltaBatchWritePlan = delegate.plan(batchId)

  def writeBatch(storageBatch: DataFrame, batchId: Long): Unit = {
    ValidatedTransactionDeltaSchema.requireExpectedSchema(storageBatch)
    val cachedBatch = storageBatch.persist()

    try {
      if (cachedBatch.isEmpty) {
        println(s"Deduplicated Delta batch skipped because it was empty: batchId=$batchId")
      } else {
        delegate.writeNonEmptyStorageBatch(cachedBatch, batchId)
      }
    } finally {
      cachedBatch.unpersist()
    }
  }
}
