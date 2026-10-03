package io.sentinelaegisforge.streaming.processing.risk

/** Only existing feature evidence is inspected; no history or monetary totals are recomputed. */
final case class RiskFeatures(
    priorTransactionCount5m: Long,
    priorAmountObservationCount: Long,
    priorAmountMean: Option[Double],
    priorAmountStddev: Option[Double],
    amountZscore: Option[Double],
    statisticalFeatureStatus: String
)

sealed trait RiskDisposition extends Product with Serializable {
  def value: String
}
object RiskDisposition {
  case object Clear extends RiskDisposition { val value = "CLEAR" }
  case object Review extends RiskDisposition { val value = "REVIEW" }
}

final case class RiskDecision(
    disposition: RiskDisposition,
    policyVersion: String,
    policyFingerprint: String,
    matchedRuleIds: Vector[String],
    reasonCodes: Vector[String]
) {
  require(matchedRuleIds.size == reasonCodes.size, "Rule IDs and reasons must align")
  def matchedRuleCount: Int = matchedRuleIds.size
}

sealed trait RiskInputError extends Product with Serializable
object RiskInputError {
  final case class NegativeCount(field: String, value: Long) extends RiskInputError
  final case class NonFiniteValue(field: String) extends RiskInputError
  final case class NegativeStandardDeviation(value: Double) extends RiskInputError
  final case class InconsistentStatisticalContext(status: String) extends RiskInputError
  final case class MissingRequiredFeature(field: String) extends RiskInputError
}

final case class RiskRule(id: String, reasonCode: String)
object RiskRules {
  val HighVelocity = RiskRule(
    "R001_HIGH_TRANSACTION_VELOCITY_5M",
    "HIGH_TRANSACTION_VELOCITY_5M"
  )
  val HighAmountZscore = RiskRule("R002_HIGH_AMOUNT_ZSCORE", "HIGH_AMOUNT_ZSCORE")
  val Combined = RiskRule("R003_VELOCITY_AND_AMOUNT_ANOMALY", "VELOCITY_AND_AMOUNT_ANOMALY")
  val ordered: Vector[RiskRule] = Vector(HighVelocity, HighAmountZscore, Combined)
}
