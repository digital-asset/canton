// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import scala.util.control.NoStackTrace

/** Errors that can arise during interpretation */
sealed abstract class SError
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable

object SError {

  /** An uncaught daml exception, to be converted into a failure status by the caller */
  final case class UnhandledException(excp: SValue.SAny) extends SError

  sealed abstract class NotAnException extends SError

  /** A malformed expression was encountered. The assumption is that the expressions are
    * type-checked and the loaded packages have been validated, hence we do not have separate errors
    * for e.g. unknown values.
    */
  final case class Crash(location: String, reason: String) extends NotAnException {
    override def getMessage: String = s"SPEEDY CRASH ($location): $reason"
  }

  /** Errors that should be reported to the user. */
  final case class InterpretationError(error: interpretation.Error) extends NotAnException {
    override def toString = productIterator.mkString(productPrefix + "(", ",", ")")
  }

}
