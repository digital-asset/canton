// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import java.io.File

import io.circe.Encoder

/** Configuration for Java keystore with optional password protection. */
case class KeyStoreConfig(path: File, password: Password)

/** Password wrapper for keystores to prevent the values being printed in logs.
  * @param pw password value - public for supporting PureConfig parsing but callers should prefer accessing through unwrap
  */
case class Password(pw: String) extends AnyVal {
  def unwrap: String = pw

  def toCharArray: Array[Char] = pw.toCharArray

  // We do not want to print out the password in log files
  override def toString: String = s"Password(****)"
}

object Password {
  // We do not want to serialize the password to JSON, e.g., as part of a config dump.
  implicit val encoder: Encoder[Password] = Encoder.encodeString.contramap(_ => "****")

  def empty: Password = Password("")
}
