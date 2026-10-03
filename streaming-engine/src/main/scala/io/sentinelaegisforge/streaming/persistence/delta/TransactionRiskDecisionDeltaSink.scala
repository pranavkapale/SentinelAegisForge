package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField}

object TransactionRiskDecisionDeltaSchema {
  val decisionFields: Vector[StructField] = Vector(
    StructField("risk_disposition", StringType, nullable = false),
    StructField("policy_version", StringType, nullable = false),
    StructField("policy_fingerprint", StringType, nullable = false),
    StructField("matched_rule_ids", ArrayType(StringType, containsNull = false), nullable = false),
    StructField("reason_codes", ArrayType(StringType, containsNull = false), nullable = false),
    StructField("matched_rule_count", IntegerType, nullable = false)
  )
  val columns = TransactionStatisticalFeatureDeltaSchema.columns ++
    decisionFields.map(f => f.name -> f.dataType)

  def check(storage: DataFrame): Unit = {
    // Delta readback relaxes nullability, including array element nullability.
    val actual = storage.schema.fields.toVector.map(f => f.name -> f.dataType)
    def normalize(values: Vector[(String, org.apache.spark.sql.types.DataType)]) = values.map {
      case (name, ArrayType(StringType, _)) => name -> ArrayType(StringType, containsNull = true)
      case other                            => other
    }
    require(normalize(actual) == normalize(columns), s"Unexpected risk decision schema: $actual")
  }
}

final class TransactionRiskDecisionDeltaSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  private val delegate = new DeltaTransactionSink(deltaPath, txnAppId, postCommitHook)
  def plan(batchId: Long): DeltaBatchWritePlan = delegate.plan(batchId)

  def writeBatch(batch: DataFrame, batchId: Long): Unit = {
    TransactionRiskDecisionDeltaSchema.check(batch)
    val storage = batch.persist()
    try {
      if (storage.isEmpty) println(s"Empty risk decision batch skipped: batchId=$batchId")
      else delegate.writeNonEmptyStorageBatch(storage, batchId)
    } finally storage.unpersist()
  }
}
