package io.sentinelaegisforge.streaming.domain.transaction

sealed trait TransactionValidationError extends Product with Serializable {
  def field: TransactionField
}

object TransactionValidationError {
  final case class MissingRequiredField(field: TransactionField) extends TransactionValidationError

  final case class MalformedEventId(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.EventId
  }

  final case class MalformedEventTime(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.EventTime
  }

  final case class MalformedIngestionTime(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.IngestionTime
  }

  final case class MalformedAmount(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.Amount
  }

  final case class NonPositiveAmount(value: BigDecimal) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.Amount
  }

  final case class InvalidCurrencyFormat(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.Currency
  }

  final case class InvalidCountryFormat(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.Country
  }

  final case class InvalidIpAddressFormat(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.IpAddress
  }

  final case class UnsupportedTransactionType(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.TransactionType
  }

  final case class MalformedSchemaVersion(value: String) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.SchemaVersion
  }

  final case class UnsupportedSchemaVersion(value: Int) extends TransactionValidationError {
    override val field: TransactionField = TransactionField.SchemaVersion
  }
}
