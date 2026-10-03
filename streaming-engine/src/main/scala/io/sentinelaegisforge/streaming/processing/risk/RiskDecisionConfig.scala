package io.sentinelaegisforge.streaming.processing.risk

import scala.util.Try

final case class RiskDecisionConfig(
    sourceDeltaPath: String,
    targetDeltaPath: String,
    checkpointLocation: String,
    deltaTxnAppId: String,
    sparkMaster: String,
    policy: RiskPolicy
) {
  require(
    Vector(sourceDeltaPath, targetDeltaPath, checkpointLocation, deltaTxnAppId, sparkMaster)
      .forall(_.trim.nonEmpty),
    "Risk runtime paths, transaction ID and master must not be blank"
  )
  require(sourceDeltaPath != targetDeltaPath, "Risk source and target must differ")
}

object RiskDecisionConfig {
  val DefaultSourceDeltaPath = ".local/delta/transaction_statistical_features_v2"
  val DefaultTargetDeltaPath = ".local/delta/transaction_risk_decisions"
  val DefaultCheckpointLocation = ".local/checkpoints/risk-decisions-v1"
  val DefaultTxnAppId = "sentinel-transaction-risk-decisions-v1"

  private val PolicyArguments = Set(
    "policy-version",
    "high-velocity-threshold-5m",
    "high-amount-zscore-threshold",
    "combined-velocity-threshold-5m",
    "combined-amount-zscore-threshold"
  )
  private val SupportedArguments = PolicyArguments ++ Set(
    "source-delta-path",
    "target-delta-path",
    "checkpoint-location",
    "delta-txn-app-id",
    "spark-master"
  )

  def parseArguments(args: Array[String]): Either[String, RiskDecisionConfig] = {
    val parsed = args.foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) {
      case (Right(values), argument) if argument.startsWith("--") && argument.contains("=") =>
        val parts = argument.drop(2).split("=", 2)
        if (!SupportedArguments.contains(parts(0))) Left(s"Unsupported argument: ${parts(0)}")
        else if (values.contains(parts(0))) Left(s"Duplicate argument: ${parts(0)}")
        else if (parts(1).isEmpty) Left(s"Empty argument: ${parts(0)}")
        else Right(values.updated(parts(0), parts(1)))
      case (Right(_), argument) => Left(s"Expected --name=value, received: $argument")
      case (left @ Left(_), _)  => left
    }
    parsed.flatMap { values =>
      val missing = PolicyArguments -- values.keySet
      if (missing.nonEmpty)
        Left(s"Explicit policy arguments required: ${missing.toVector.sorted.mkString(", ")}")
      else
        Try {
          RiskDecisionConfig(
            values.getOrElse("source-delta-path", DefaultSourceDeltaPath),
            values.getOrElse("target-delta-path", DefaultTargetDeltaPath),
            values.getOrElse("checkpoint-location", DefaultCheckpointLocation),
            values.getOrElse("delta-txn-app-id", DefaultTxnAppId),
            values.getOrElse("spark-master", "local[*]"),
            RiskPolicy(
              values("policy-version"),
              values("high-velocity-threshold-5m").toLong,
              values("high-amount-zscore-threshold").toDouble,
              values("combined-velocity-threshold-5m").toLong,
              values("combined-amount-zscore-threshold").toDouble
            )
          )
        }.toEither.left.map(error => s"Invalid risk configuration: ${error.getMessage}")
    }
  }
}
