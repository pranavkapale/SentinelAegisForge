package io.sentinelaegisforge.streaming.processing.risk

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat

/** Explicit engineering policy, not calibrated production fraud thresholds. */
final case class RiskPolicy(
    policyVersion: String,
    highVelocityThreshold5m: Long,
    highAmountZScoreThreshold: Double,
    combinedVelocityThreshold5m: Long,
    combinedAmountZScoreThreshold: Double
) {
  require(
    policyVersion != null && policyVersion.nonEmpty && policyVersion == policyVersion.trim &&
      !policyVersion.exists(_.isControl),
    "policyVersion must be nonblank, trimmed and contain no control characters"
  )
  require(highVelocityThreshold5m >= 1L, "highVelocityThreshold5m must be >= 1")
  require(combinedVelocityThreshold5m >= 1L, "combinedVelocityThreshold5m must be >= 1")
  require(
    highAmountZScoreThreshold.isFinite && highAmountZScoreThreshold > 0.0,
    "highAmountZScoreThreshold must be positive and finite"
  )
  require(
    combinedAmountZScoreThreshold.isFinite && combinedAmountZScoreThreshold > 0.0,
    "combinedAmountZScoreThreshold must be positive and finite"
  )

  // UTF-8, LF-terminated lines; exact double values use JDK hexadecimal representation.
  // The version's UTF-8 byte length makes its representation unambiguous.
  val canonicalRepresentation: String = Vector(
    "decision_logic_version=deterministic-risk-v1",
    s"policy_version=${policyVersion.getBytes(UTF_8).length}:$policyVersion",
    "decision_mapping=no_matches:CLEAR;any_match:REVIEW",
    s"${RiskRules.HighVelocity.id}|${RiskRules.HighVelocity.reasonCode}|prior_transaction_count_5m>=${highVelocityThreshold5m}",
    s"${RiskRules.HighAmountZscore.id}|${RiskRules.HighAmountZscore.reasonCode}|status=READY;z_present;amount_zscore>=${java.lang.Double.toHexString(highAmountZScoreThreshold)}",
    s"${RiskRules.Combined.id}|${RiskRules.Combined.reasonCode}|status=READY;z_present;prior_transaction_count_5m>=${combinedVelocityThreshold5m};amount_zscore>=${java.lang.Double.toHexString(combinedAmountZScoreThreshold)}"
  ).mkString("", "\n", "\n")

  val policyFingerprint: String = HexFormat
    .of()
    .formatHex(
      MessageDigest.getInstance("SHA-256").digest(canonicalRepresentation.getBytes(UTF_8))
    )
}
