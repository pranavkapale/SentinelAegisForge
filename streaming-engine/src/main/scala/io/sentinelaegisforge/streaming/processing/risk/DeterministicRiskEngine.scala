package io.sentinelaegisforge.streaming.processing.risk

import io.sentinelaegisforge.streaming.processing.statisticalfeatures.CustomerAmountStatisticsProcessor

object DeterministicRiskEngine {
  import RiskInputError._

  /** No I/O, clock, randomness, feature mutation or customer state. */
  def evaluate(
      input: RiskFeatures,
      policy: RiskPolicy
  ): Either[Vector[RiskInputError], RiskDecision] = {
    val errors = validate(input)
    if (errors.nonEmpty) Left(errors)
    else {
      val ready = input.statisticalFeatureStatus == "READY"
      val conditions = Vector(
        input.priorTransactionCount5m >= policy.highVelocityThreshold5m,
        ready && input.amountZscore.exists(_ >= policy.highAmountZScoreThreshold),
        ready && input.priorTransactionCount5m >= policy.combinedVelocityThreshold5m &&
          input.amountZscore.exists(_ >= policy.combinedAmountZScoreThreshold)
      )
      val matched = RiskRules.ordered.zip(conditions).collect { case (rule, true) => rule }
      Right(
        RiskDecision(
          if (matched.isEmpty) RiskDisposition.Clear else RiskDisposition.Review,
          policy.policyVersion,
          policy.policyFingerprint,
          matched.map(_.id),
          matched.map(_.reasonCode)
        )
      )
    }
  }

  private def validate(input: RiskFeatures): Vector[RiskInputError] = {
    val errors = Vector.newBuilder[RiskInputError]
    if (input.priorTransactionCount5m < 0L)
      errors += NegativeCount("prior_transaction_count_5m", input.priorTransactionCount5m)
    if (input.priorAmountObservationCount < 0L)
      errors += NegativeCount("prior_amount_observation_count", input.priorAmountObservationCount)
    Vector(
      "prior_amount_mean" -> input.priorAmountMean,
      "prior_amount_stddev" -> input.priorAmountStddev,
      "amount_zscore" -> input.amountZscore
    ).foreach { case (field, value) =>
      if (value.exists(!_.isFinite)) errors += NonFiniteValue(field)
    }
    input.priorAmountStddev.filter(_ < 0.0).foreach(v => errors += NegativeStandardDeviation(v))
    val count = input.priorAmountObservationCount
    val floor = CustomerAmountStatisticsProcessor.StandardDeviationFloor
    val consistent = input.statisticalFeatureStatus match {
      case "NO_HISTORY" =>
        count == 0L && input.priorAmountMean.isEmpty && input.priorAmountStddev.isEmpty &&
        input.amountZscore.isEmpty
      case "INSUFFICIENT_VARIANCE_HISTORY" =>
        count == 1L && input.priorAmountMean.isDefined && input.priorAmountStddev.isEmpty &&
        input.amountZscore.isEmpty
      case "ZERO_VARIANCE" =>
        count >= 2L && input.priorAmountMean.isDefined &&
        input.priorAmountStddev.exists(v => v >= 0.0 && v <= floor) && input.amountZscore.isEmpty
      case "READY" =>
        count >= 2L && input.priorAmountMean.isDefined &&
        input.priorAmountStddev.exists(_ > floor) && input.amountZscore.isDefined
      case _ => false
    }
    if (!consistent) errors += InconsistentStatisticalContext(input.statisticalFeatureStatus)
    errors.result()
  }
}
