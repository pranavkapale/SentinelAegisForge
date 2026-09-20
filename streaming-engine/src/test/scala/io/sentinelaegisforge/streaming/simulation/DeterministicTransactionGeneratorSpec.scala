package io.sentinelaegisforge.streaming.simulation

import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator,
  TransactionType
}

final class DeterministicTransactionGeneratorSpec extends AnyFunSuite {
  test("identical configuration produces identical candidate sequences") {
    val first = new DeterministicTransactionGenerator(config).generateValidCandidates(20)
    val second = new DeterministicTransactionGenerator(config).generateValidCandidates(20)
    assert(first == second)
  }

  test("a different seed changes generated transaction identity") {
    val first = new DeterministicTransactionGenerator(config).generateValidCandidates(5)
    val second = new DeterministicTransactionGenerator(config.copy(seed = 43L))
      .generateValidCandidates(5)

    assert(first.map(_.eventId) != second.map(_.eventId))
    assert(first.map(_.transactionId) != second.map(_.transactionId))
  }

  test("generated candidates validate as version 1 card payments with deterministic times") {
    val candidates = new DeterministicTransactionGenerator(config).generateValidCandidates(10)

    candidates.zipWithIndex.foreach { case (candidate, index) =>
      val event = validated(candidate)
      val expectedEventTime = config.baseEventTime.plus(config.eventSpacing.multipliedBy(index))
      assert(event.eventTime == expectedEventTime)
      assert(event.ingestionTime == expectedEventTime.plus(config.ingestionDelay))
      assert(event.amount > 0)
      assert(event.transactionType == TransactionType.CardPayment)
      assert(event.schemaVersion == 1)
    }
  }

  test("each named invalid scenario fails for its expected typed reason") {
    val valid = new DeterministicTransactionGenerator(config).generateValidCandidates(1).head

    IntentionalInvalidTransactionScenarios.all(valid).foreach { scenario =>
      TransactionEventValidator.validate(scenario.candidate) match {
        case Left(errors) =>
          assert(
            errors.contains(scenario.expectedError),
            s"${scenario.name} produced $errors instead of ${scenario.expectedError}"
          )
        case Right(event) => fail(s"${scenario.name} unexpectedly produced $event")
      }
    }
  }

  private def validated(
      candidate: io.sentinelaegisforge.streaming.domain.transaction.TransactionEventCandidate
  ): TransactionEvent =
    TransactionEventValidator.validate(candidate) match {
      case Right(event) => event
      case Left(errors) => fail(s"Generated candidate did not validate: $errors")
    }

  private val config = TransactionGeneratorConfig(
    seed = 42L,
    baseEventTime = Instant.parse("2030-01-01T00:00:00Z"),
    eventSpacing = Duration.ofSeconds(15),
    ingestionDelay = Duration.ofSeconds(3)
  )
}
