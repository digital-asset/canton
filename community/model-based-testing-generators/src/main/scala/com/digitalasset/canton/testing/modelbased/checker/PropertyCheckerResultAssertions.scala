// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.checker

import com.digitalasset.canton.testing.modelbased.checker.PropertyChecker.{
  CheckFailed,
  CheckPassed,
  CheckResult,
}
import org.scalatest.Assertions
import org.scalatest.compatible.Assertion

/** Mix in this trait to get an extension method on `CheckResult` that turns a check failure into a
  * scalatest failure with informative clues.
  */
trait PropertyCheckerResultAssertions { this: Assertions =>

  implicit class CheckResultOps[A](result: CheckResult[A]) {

    /** Asserts that the check passed. On failure, reports the original value, original error,
      * shrunk value, and shrunk error as nested scalatest clues.
      *
      * @param render
      *   a function to render values of type `A` as strings for error messages
      */
    def assertPassed(render: A => String): Assertion =
      result match {
        case _: CheckPassed => succeed
        case CheckFailed(originalValue, originalError, shrinkResult, _, _) =>
          withClue(s"${result.summary}\n") {
            withClue(s"\nOriginal value:\n${render(originalValue)}\n") {
              withClue(s"\nOriginal error:\n$originalError\n") {
                withClue(s"\nShrunk value:\n${render(shrinkResult.value)}\n") {
                  fail(shrinkResult.error)
                }
              }
            }
          }
      }
  }
}
