// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.LookupError

package object refinement {

  @inline
  private[refinement] def safelyRun[X](
      handleMissingPackages: => Result[?]
  )(unsafeRun: => X): Result[X] = {

    def start(first: Boolean): Result[X] =
      try {
        Result.done(unsafeRun)
      } catch {
        case Error.Preprocessing.Lookup(LookupError.MissingPackage(_, _)) if first =>
          handleMissingPackages.flatMap(_ => start(false))
        case e: Error.Preprocessing.Error =>
          Result.error(e)
      }

    start(first = true)
  }

  @inline
  private[refinement] def safelyRun[X](unsafeRun: => X): Either[Error.Preprocessing.Error, X] =
    try {
      Right(unsafeRun)
    } catch {
      case e: Error.Preprocessing.Error =>
        Left(e)
    }

  @throws[Error.Preprocessing.Error]
  private[refinement] def handleLookup[X](either: Either[LookupError, X]): X = either match {
    case Right(v) => v
    case Left(error) => throw Error.Preprocessing.Lookup(error)
  }

}
