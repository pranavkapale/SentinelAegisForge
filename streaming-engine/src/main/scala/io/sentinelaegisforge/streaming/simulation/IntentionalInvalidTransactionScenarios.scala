package io.sentinelaegisforge.streaming.simulation

import io.sentinelaegisforge.streaming.domain.transaction.TransactionField.CustomerId
import io.sentinelaegisforge.streaming.domain.transaction.TransactionValidationError._
import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEventCandidate,
  TransactionValidationError
}

final case class IntentionalInvalidTransactionScenario(
    name: String,
    candidate: TransactionEventCandidate,
    expectedError: TransactionValidationError
)

object IntentionalInvalidTransactionScenarios {
  def all(valid: TransactionEventCandidate): Vector[IntentionalInvalidTransactionScenario] =
    Vector(
      IntentionalInvalidTransactionScenario(
        name = "blank-customer-id",
        candidate = valid.copy(customerId = Some("  ")),
        expectedError = MissingRequiredField(CustomerId)
      ),
      IntentionalInvalidTransactionScenario(
        name = "malformed-event-uuid",
        candidate = valid.copy(eventId = Some("not-a-uuid")),
        expectedError = MalformedEventId("not-a-uuid")
      ),
      IntentionalInvalidTransactionScenario(
        name = "invalid-monetary-amount",
        candidate = valid.copy(amount = Some("-1.00")),
        expectedError = NonPositiveAmount(BigDecimal("-1.00"))
      ),
      IntentionalInvalidTransactionScenario(
        name = "malformed-currency",
        candidate = valid.copy(currency = Some("usd")),
        expectedError = InvalidCurrencyFormat("usd")
      ),
      IntentionalInvalidTransactionScenario(
        name = "unsupported-transaction-type",
        candidate = valid.copy(transactionType = Some("WIRE_TRANSFER")),
        expectedError = UnsupportedTransactionType("WIRE_TRANSFER")
      ),
      IntentionalInvalidTransactionScenario(
        name = "unsupported-schema-version",
        candidate = valid.copy(schemaVersion = Some("2")),
        expectedError = UnsupportedSchemaVersion(2)
      ),
      IntentionalInvalidTransactionScenario(
        name = "malformed-event-timestamp",
        candidate = valid.copy(eventTime = Some("yesterday")),
        expectedError = MalformedEventTime("yesterday")
      )
    )
}
