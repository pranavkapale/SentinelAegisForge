package io.sentinelaegisforge.streaming.processing.risk

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types.StructType

import io.sentinelaegisforge.streaming.persistence.delta.{
  TransactionRiskDecisionDeltaSchema,
  TransactionStatisticalFeatureDeltaSchema
}

final class InvalidRiskFeaturesException(eventId: String, errors: Vector[RiskInputError])
    extends IllegalArgumentException(
      s"Invalid risk features for event_id=$eventId: ${errors.mkString(", ")}"
    )

object RiskDecisionTransformer {
  def transform(source: DataFrame, policy: RiskPolicy): DataFrame = {
    val actual = source.schema.fields.toVector.map(f => f.name -> f.dataType)
    require(
      actual == TransactionStatisticalFeatureDeltaSchema.columns,
      s"Unexpected v2 risk input schema: $actual"
    )
    val schema = StructType(
      source.schema.fields ++ TransactionRiskDecisionDeltaSchema.decisionFields
    )
    // Row encoding retains the source decimals (including decimal(38,4)), nulls and lineage
    // unchanged rather than passing money through a product encoder's inferred decimal scale.
    source.map { row =>
      val required = Vector(
        "prior_transaction_count_5m",
        "prior_amount_observation_count",
        "statistical_feature_status"
      )
      val missing = required
        .filter(name => row.isNullAt(row.fieldIndex(name)))
        .map(RiskInputError.MissingRequiredFeature)
      val eventId = row.getAs[String]("event_id")
      if (missing.nonEmpty) throw new InvalidRiskFeaturesException(eventId, missing)
      def optionalDouble(name: String): Option[Double] =
        Option(row.getAs[java.lang.Double](name)).map(_.doubleValue())
      val input = RiskFeatures(
        row.getAs[Long]("prior_transaction_count_5m"),
        row.getAs[Long]("prior_amount_observation_count"),
        optionalDouble("prior_amount_mean"),
        optionalDouble("prior_amount_stddev"),
        optionalDouble("amount_zscore"),
        row.getAs[String]("statistical_feature_status")
      )
      val decision = DeterministicRiskEngine
        .evaluate(input, policy)
        .fold(errors => throw new InvalidRiskFeaturesException(eventId, errors), identity)
      Row.fromSeq(
        row.toSeq ++ Vector(
          decision.disposition.value,
          decision.policyVersion,
          decision.policyFingerprint,
          decision.matchedRuleIds,
          decision.reasonCodes,
          decision.matchedRuleCount
        )
      )
    }(Encoders.row(schema))
  }
}
