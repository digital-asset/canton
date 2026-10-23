// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import scala.util.parsing.input.{NoPosition, Position}

private[daml] sealed abstract class ParserError(val description: String)
    extends RuntimeException(description) {
  def position: Position
}

private[daml] object ParserError {
  private[parser] def describe(message: String, position: Position): String =
    position match {
      case NoPosition => message
      case _ =>
        s"$message at line ${position.line}, column ${position.column}\n${position.longString}"
    }
}

private[daml] final case class LexingError(message: String, position: Position = NoPosition)
    extends ParserError(ParserError.describe(message, position)) {
  override def toString: String = s"ParsingError($description)"
}

private[daml] final case class ParsingError(message: String, position: Position)
    extends ParserError(ParserError.describe(message, position)) {
  override def toString: String = s"ParsingError($description)"
}

private[daml] final case class CallSite(file: String, line: Int)

private[daml] final case class InterpolationError(cause: ParserError, callSite: Option[CallSite])
    extends RuntimeException(InterpolationError.describe(cause, callSite), cause)

private[daml] object InterpolationError {

  private def describe(cause: ParserError, callSite: Option[CallSite]): String =
    callSite.fold(cause.description) { case CallSite(file, line) =>
      val where = cause.position match {
        case NoPosition => s"string parsed at $file:$line"
        case position => s"error at $file:${line + position.line - 1}"
      }
      s"${cause.description}\n($where)"
    }
}
