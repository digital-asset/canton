// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object data {

  @throws[IllegalArgumentException]
  def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

  val Numeric: NumericModule = new NumericModule {
    override type Numeric = java.math.BigDecimal
    @inline
    override private[data] def cast(x: java.math.BigDecimal): Numeric = x
  }
  type Numeric = Numeric.Numeric

  val Uint256: Uint256Module = new Uint256Module {
    override type Uint256 = java.math.BigInteger
    @inline
    override private[data] def cast(x: java.math.BigInteger): Uint256 = x
    override val ZERO: Uint256 = java.math.BigInteger.ZERO
    override val ONE: Uint256 = java.math.BigInteger.ONE
    override val MaxValue: Uint256 = MODULUS.subtract(java.math.BigInteger.ONE)
  }
  type Uint256 = Uint256.Uint256

  type Relation[A, B] = Map[A, Set[B]]

  val Text: TextModule = TextModuleImpl
  type Text = Text.Text

}
