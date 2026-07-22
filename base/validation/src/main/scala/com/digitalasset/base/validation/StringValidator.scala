// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.validation

/** A reason a string is unsafe to persist. [[message]] never echoes the (possibly malformed or
  * sensitive) content — only the offending position.
  */
sealed trait StringViolation {
  def message: String
}

object StringViolation {
  final case class NulCharacter(index: Int) extends StringViolation {
    override def message: String = s"it contains a NUL character (U+0000) at index $index"
  }

  final case class ControlCharacter(index: Int, codePoint: Int) extends StringViolation {
    override def message: String =
      f"it contains a control character (U+$codePoint%04X) at index $index"
  }

  final case class UnpairedHighSurrogate(index: Int) extends StringViolation {
    override def message: String = s"it contains an unpaired high surrogate at index $index"
  }

  final case class UnpairedLowSurrogate(index: Int) extends StringViolation {
    override def message: String = s"it contains an unpaired low surrogate at index $index"
  }
}

object StringValidator {
  import StringViolation.*

  private val ok: Either[StringViolation, Unit] = Right(())

  /** Validates a given string.
    *
    * Validation includes:
    *   - Valid UTF16 encoding (no unpaired surrogates) as otherwise roundtrip via Postgres will
    *     return different strings with unpaired surrogates replaced by `?`.
    *   - No NUL characters (U+0000) as Postgres cannot persist strings with NUL characters.
    *   - No escape control characters other than regular whitespace (tab, line feed, carriage
    *     return) as they can cause issues with pretty printing.
    */
  def validate(str: String): Either[StringViolation, Unit] = {
    val length = str.length

    def isAllowedWhitespace(ch: Char): Boolean = ch == '\t' || ch == '\n' || ch == '\r'

    @scala.annotation.tailrec
    def loop(i: Int): Either[StringViolation, Unit] =
      if (i >= length) ok
      else {
        val ch = str.charAt(i)
        if (ch == '\u0000') Left(NulCharacter(i))
        else if (Character.isISOControl(ch) && !isAllowedWhitespace(ch))
          Left(ControlCharacter(i, ch.toInt))
        else if (Character.isHighSurrogate(ch)) {
          if (i + 1 >= length || !Character.isLowSurrogate(str.charAt(i + 1)))
            Left(UnpairedHighSurrogate(i))
          else loop(i + 2)
        } else if (Character.isLowSurrogate(ch)) Left(UnpairedLowSurrogate(i))
        else loop(i + 1)
      }

    loop(0)
  }
}
