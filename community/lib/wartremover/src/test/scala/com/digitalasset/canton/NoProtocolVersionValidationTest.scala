// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.digitalasset.canton.NoProtocolVersionValidationTest.*
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class NoProtocolVersionValidationTest extends AnyWordSpec with Matchers {

  private def assertErrors(result: WartTestTraverser.Result, expected: Int): Assertion = {
    result.errors.length shouldBe expected
    result.errors.foreach(_ should include(NoProtocolVersionValidation.message))
    succeed
  }

  "NoProtocolVersionValidation" should {
    "detect a qualified reference to NoValidation" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        val pvv: ProtocolVersionValidation = ProtocolVersionValidation.NoValidation
        pvv
      }
      assertErrors(result, 1)
    }

    "detect an imported reference to NoValidation" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        import ProtocolVersionValidation.NoValidation
        Seq[ProtocolVersionValidation](NoValidation)
      }
      assertErrors(result, 1)
    }

    "detect NoValidation in the body of a case" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        (ProtocolVersionValidation.AlwaysValidation: ProtocolVersionValidation) match {
          case ProtocolVersionValidation.AlwaysValidation => ProtocolVersionValidation.NoValidation
          case other => other
        }
      }
      assertErrors(result, 1)
    }

    "allow pattern matches on NoValidation" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        (ProtocolVersionValidation.AlwaysValidation: ProtocolVersionValidation) match {
          case ProtocolVersionValidation.NoValidation => false
          case ProtocolVersionValidation.AlwaysValidation => true
        }
      }
      assertErrors(result, 0)
    }

    "allow other validations" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        val pvv: ProtocolVersionValidation = ProtocolVersionValidation.AlwaysValidation
        pvv
      }
      assertErrors(result, 0)
    }

    "respect suppressions" in {
      val result = WartTestTraverser(NoProtocolVersionValidation) {
        @SuppressWarnings(Array("com.digitalasset.canton.NoProtocolVersionValidation"))
        val pvv: ProtocolVersionValidation = ProtocolVersionValidation.NoValidation
        pvv
      }
      assertErrors(result, 0)
    }
  }
}

object NoProtocolVersionValidationTest {

  /** Stand-in for the real `ProtocolVersionValidation`, which this project cannot depend on. */
  sealed trait ProtocolVersionValidation extends Product with Serializable

  object ProtocolVersionValidation {
    case object NoValidation extends ProtocolVersionValidation
    case object AlwaysValidation extends ProtocolVersionValidation
  }

}
