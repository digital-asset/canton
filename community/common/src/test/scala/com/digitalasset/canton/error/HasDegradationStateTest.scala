// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{ErrorCategory, ErrorClass, ErrorCode, ErrorGroup}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.error.Errors.{ErrorForTesting, TestError}
import com.digitalasset.canton.logging.{ErrorLoggingContext, SuppressionRule}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level.INFO

object Errors extends ErrorGroup()(ErrorClass.root()) {

  abstract class ErrorForTesting(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends CantonError()

  object TestError extends ErrorCode("TEST_ERROR", ErrorCategory.ContentionOnSharedResources) {
    case class Error(num: Int)(implicit val loggingContext: ErrorLoggingContext)
        extends ErrorForTesting(s"caws $num")

    override def errorConveyanceDocString: Option[String] = None
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class HasDegradationStateTest extends AnyWordSpec with BaseTest {

  case class ClassWithDegradationState() extends HasDegradationState[ErrorForTesting] {}

  "A class that implements HasErrorState" should {
    val instance = ClassWithDegradationState()

    "track the occurred errors" in {
      instance.degradationOccurred(TestError.Error(1))
    }

    "log the given message if an already occurred error is resolved" in {
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(INFO))(
        instance.resolveDegradationIfExists((err: ErrorForTesting) => "resolution message"),
        forEvery(_) {
          _.message should (include(TestError.Error(1).code.id) and include("resolution message"))
        },
      )
    }
  }
}
