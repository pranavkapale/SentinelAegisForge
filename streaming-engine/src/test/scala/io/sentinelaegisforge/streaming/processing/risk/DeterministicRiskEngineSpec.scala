package io.sentinelaegisforge.streaming.processing.risk

import org.scalatest.funsuite.AnyFunSuite

final class DeterministicRiskEngineSpec extends AnyFunSuite {
  private val policy = RiskPolicy("phase11-verification-v1", 3L, 2.0, 3L, 1.0)
  private def ready(velocity: Long = 0L, z: Double = 0.5) =
    RiskFeatures(velocity, 2L, Some(100.0), Some(10.0), Some(z), "READY")
  private def decision(input: RiskFeatures, p: RiskPolicy = policy) =
    DeterministicRiskEngine.evaluate(input, p).fold(e => fail(e.toString), identity)

  test("low velocity and non-anomalous READY statistics produce CLEAR and empty explanations") {
    val result = decision(ready())
    assert(result.disposition == RiskDisposition.Clear)
    assert(result.matchedRuleIds.isEmpty && result.reasonCodes.isEmpty)
    assert(result.matchedRuleCount == 0)
    assert(result.policyVersion == policy.policyVersion)
    assert(result.policyFingerprint == policy.policyFingerprint)
  }

  test("R001 uses the inclusive customer-wide velocity threshold without statistical readiness") {
    val result = decision(RiskFeatures(3L, 0L, None, None, None, "NO_HISTORY"))
    assert(result.disposition == RiskDisposition.Review)
    assert(result.matchedRuleIds == Vector(RiskRules.HighVelocity.id))
    assert(result.reasonCodes == Vector(RiskRules.HighVelocity.reasonCode))
    assert(decision(ready(velocity = 2L)).matchedRuleIds.isEmpty)
  }

  test("R002 matches the inclusive positive anomaly threshold with low velocity") {
    val result = decision(ready(z = 2.0))
    assert(result.disposition == RiskDisposition.Review)
    assert(result.matchedRuleIds == Vector(RiskRules.HighAmountZscore.id))
    assert(result.reasonCodes == Vector(RiskRules.HighAmountZscore.reasonCode))
    assert(decision(ready(z = 1.999)).matchedRuleIds.isEmpty)
  }

  test("R003 composes velocity and positive anomaly and retains independently matching R001") {
    val result = decision(ready(3L, 1.0))
    assert(result.matchedRuleIds == Vector(RiskRules.HighVelocity.id, RiskRules.Combined.id))
    val isolated = decision(ready(3L, 1.0), policy.copy(highVelocityThreshold5m = 10L))
    assert(isolated.matchedRuleIds == Vector(RiskRules.Combined.id))
    assert(decision(ready(2L, 1.0)).matchedRuleIds.isEmpty)
    assert(decision(ready(3L, 0.999)).matchedRuleIds == Vector(RiskRules.HighVelocity.id))
  }

  test("all explanations are retained in canonical R001 R002 R003 order") {
    val result = decision(ready(3L, 2.0))
    assert(result.matchedRuleIds == RiskRules.ordered.map(_.id))
    assert(result.reasonCodes == RiskRules.ordered.map(_.reasonCode))
    assert(result.matchedRuleCount == 3)
  }

  test("NO_HISTORY INSUFFICIENT_VARIANCE_HISTORY and ZERO_VARIANCE do not trigger z-score rules") {
    val inputs = Vector(
      RiskFeatures(0L, 0L, None, None, None, "NO_HISTORY"),
      RiskFeatures(0L, 1L, Some(100.0), None, None, "INSUFFICIENT_VARIANCE_HISTORY"),
      RiskFeatures(0L, 2L, Some(100.0), Some(0.0), None, "ZERO_VARIANCE")
    )
    inputs.foreach(input => assert(decision(input).disposition == RiskDisposition.Clear))
    inputs.foreach(input =>
      assert(DeterministicRiskEngine.evaluate(input.copy(amountZscore = Some(9.0)), policy).isLeft)
    )
  }

  test("negative anomaly is never converted to an absolute positive anomaly") {
    assert(decision(ready(z = -10.0)).disposition == RiskDisposition.Clear)
    assert(decision(ready(3L, -10.0)).matchedRuleIds == Vector(RiskRules.HighVelocity.id))
  }

  test("evaluation is deterministic and does not mutate input evidence") {
    val input = ready(4L, 5.0)
    val first = decision(input)
    (1 to 20).foreach(_ => assert(decision(input) == first))
    assert(input == ready(4L, 5.0))
  }

  test(
    "fingerprint is stable and canonical double thresholds have exact hexadecimal representation"
  ) {
    assert(policy.copy().policyFingerprint == policy.policyFingerprint)
    assert(
      policy.policyFingerprint == "594b7caf5f937e6098c4fb0ca3ea692759600c40461a26954a3e1012e2b9635e"
    )
    assert(policy.policyFingerprint.matches("[0-9a-f]{64}"))
    assert(
      policy.canonicalRepresentation.startsWith("decision_logic_version=deterministic-risk-v1\n")
    )
    assert(policy.canonicalRepresentation.contains("policy_version=23:phase11-verification-v1\n"))
    assert(policy.canonicalRepresentation.contains("amount_zscore>=0x1.0p1"))
    assert(policy.canonicalRepresentation.endsWith("\n"))
    assert(
      policy.canonicalRepresentation.indexOf(RiskRules.HighVelocity.id) <
        policy.canonicalRepresentation.indexOf(RiskRules.HighAmountZscore.id)
    )
  }

  test("every threshold and human semantic version participate in policy identity") {
    Vector(
      policy.copy(highVelocityThreshold5m = 4L),
      policy.copy(highAmountZScoreThreshold = 2.1),
      policy.copy(combinedVelocityThreshold5m = 4L),
      policy.copy(combinedAmountZScoreThreshold = 1.1),
      policy.copy(policyVersion = "phase11-verification-v2")
    ).foreach(changed => assert(changed.policyFingerprint != policy.policyFingerprint))
  }

  test("blank or ambiguous policy versions and nonpositive count thresholds are rejected") {
    Vector("", " ", " v1", "v1\n").foreach(v =>
      intercept[IllegalArgumentException](policy.copy(policyVersion = v))
    )
    Vector(0L, -1L).foreach { n =>
      intercept[IllegalArgumentException](policy.copy(highVelocityThreshold5m = n))
      intercept[IllegalArgumentException](policy.copy(combinedVelocityThreshold5m = n))
    }
  }

  test("both z-score thresholds must be positive and finite") {
    Vector(0.0, -1.0, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { z =>
      intercept[IllegalArgumentException](policy.copy(highAmountZScoreThreshold = z))
      intercept[IllegalArgumentException](policy.copy(combinedAmountZScoreThreshold = z))
    }
  }

  test("negative counts and nonfinite feature values produce typed errors rather than decisions") {
    val bad = ready().copy(
      priorTransactionCount5m = -1L,
      priorAmountObservationCount = -2L,
      amountZscore = Some(Double.NaN)
    )
    val errors = DeterministicRiskEngine.evaluate(bad, policy).swap.toOption.get
    assert(errors.contains(RiskInputError.NegativeCount("prior_transaction_count_5m", -1L)))
    assert(errors.contains(RiskInputError.NegativeCount("prior_amount_observation_count", -2L)))
    assert(errors.contains(RiskInputError.NonFiniteValue("amount_zscore")))
    Vector(Double.PositiveInfinity, Double.NegativeInfinity).foreach { n =>
      assert(DeterministicRiskEngine.evaluate(ready(z = n), policy).isLeft)
    }
    assert(
      DeterministicRiskEngine
        .evaluate(ready().copy(priorAmountMean = Some(Double.NaN)), policy)
        .isLeft
    )
    assert(
      DeterministicRiskEngine
        .evaluate(ready().copy(priorAmountStddev = Some(Double.PositiveInfinity)), policy)
        .isLeft
    )
  }

  test(
    "unknown status missing READY z-score and invalid standard deviation cannot be reinterpreted"
  ) {
    Vector(
      ready().copy(statisticalFeatureStatus = "UNKNOWN"),
      ready().copy(amountZscore = None),
      ready().copy(priorAmountMean = None),
      ready().copy(priorAmountStddev = Some(0.0)),
      ready().copy(priorAmountStddev = Some(-1.0)),
      ready().copy(priorAmountObservationCount = 1L),
      ready().copy(statisticalFeatureStatus = "ZERO_VARIANCE")
    ).foreach(input => assert(DeterministicRiskEngine.evaluate(input, policy).isLeft))
  }

  test(
    "runnable application requires explicit policy and validates unknown duplicate and malformed arguments"
  ) {
    assert(RiskDecisionConfig.parseArguments(Array.empty).isLeft)
    val arguments = Array(
      "--policy-version=demo-v1",
      "--high-velocity-threshold-5m=3",
      "--high-amount-zscore-threshold=2",
      "--combined-velocity-threshold-5m=3",
      "--combined-amount-zscore-threshold=1"
    )
    val config = RiskDecisionConfig.parseArguments(arguments).fold(fail(_), identity)
    assert(config.sourceDeltaPath.endsWith("transaction_statistical_features_v2"))
    assert(config.checkpointLocation.endsWith("risk-decisions-v1"))
    assert(config.deltaTxnAppId == "sentinel-transaction-risk-decisions-v1")
    assert(RiskDecisionConfig.parseArguments(arguments :+ "--unknown=x").isLeft)
    assert(RiskDecisionConfig.parseArguments(arguments :+ "--policy-version=duplicate").isLeft)
    assert(
      RiskDecisionConfig
        .parseArguments(arguments.map(_.replace("threshold=2", "threshold=NaN")))
        .isLeft
    )
    assert(RiskDecisionConfig.parseArguments(arguments.map(_.replace("5m=3", "5m=bad"))).isLeft)
  }
}
