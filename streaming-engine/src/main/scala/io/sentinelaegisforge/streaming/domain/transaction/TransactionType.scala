package io.sentinelaegisforge.streaming.domain.transaction

sealed trait TransactionType extends Product with Serializable {
  def externalName: String
}

object TransactionType {
  case object CardPayment extends TransactionType {
    override val externalName: String = "CARD_PAYMENT"
  }

  val supported: Vector[TransactionType] = Vector(CardPayment)

  def fromExternalName(value: String): Option[TransactionType] =
    supported.find(_.externalName == value)
}
