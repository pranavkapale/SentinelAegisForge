package io.sentinelaegisforge.streaming.processing.risk

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.TransactionRiskDecisionDeltaSink

object TransactionRiskDecisionApp {
  def main(args: Array[String]): Unit = {
    val config = RiskDecisionConfig
      .parseArguments(args)
      .fold(message => throw new IllegalArgumentException(message), identity)
    val spark = TransactionSparkSession
      .builder(config.sparkMaster)
      .appName("transaction-risk-decisions")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      require(
        DeltaTable.isDeltaTable(spark, config.sourceDeltaPath),
        s"No corrected statistical-feature Delta source exists at ${config.sourceDeltaPath}"
      )
      println(
        s"Explicit engineering policy (NOT production-calibrated): version=${config.policy.policyVersion} fingerprint=${config.policy.policyFingerprint}\n${config.policy.canonicalRepresentation}"
      )
      val source = spark.readStream.format("delta").load(config.sourceDeltaPath)
      val decisions = RiskDecisionTransformer.transform(source, config.policy)
      val sink = new TransactionRiskDecisionDeltaSink(config.targetDeltaPath, config.deltaTxnAppId)
      val query = decisions.writeStream
        .queryName("transaction-risk-decisions")
        .option("checkpointLocation", config.checkpointLocation)
        .trigger(Trigger.AvailableNow())
        .foreachBatch((batch: DataFrame, batchId: Long) => sink.writeBatch(batch, batchId))
        .start()
      query.awaitTermination()
      query.recentProgress.foreach(p =>
        println(
          s"Risk decision progress: batchId=${p.batchId} inputRows=${p.numInputRows} stateOperators=${p.stateOperators.length}"
        )
      )
      println(
        s"Risk decisions completed: source=${config.sourceDeltaPath} target=${config.targetDeltaPath} checkpoint=${config.checkpointLocation} txnAppId=${config.deltaTxnAppId}"
      )
    } finally spark.stop()
  }
}
