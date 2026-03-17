// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.runner

import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands

/** Error types common to both [[ReferenceInterpreter]] and [[CantonInterpreter]]. */
object InterpretationErrors {
  sealed trait InterpreterError {
    def pretty: String = this match {
      case TranslationError(error) => error.toString
      case SubmitFailure(failure) => failure
    }
  }
  final case class TranslationError(error: ConcreteToCommands.TranslationError)
      extends InterpreterError
  final case class SubmitFailure(failure: String) extends InterpreterError

}
