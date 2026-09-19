package io.sentinelaegisforge.streaming

import org.scalatest.funsuite.AnyFunSuite

/** Proves that the Scala test wiring executes correctly.
  *
  * Remove this smoke test once substantive domain tests provide the same build coverage.
  */
final class FoundationSmokeSpec extends AnyFunSuite {
  test("the Scala foundation test suite executes") {
    assert(1 + 1 == 2)
  }
}
