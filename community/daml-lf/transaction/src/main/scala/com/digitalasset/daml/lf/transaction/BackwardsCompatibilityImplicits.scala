// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.value.Value.ContractId

// These are provided to support the implementation of the legacy state machine and transaction tree, used in PV34.
object BackwardsCompatibilityImplicits {

  implicit class CidVectorOps(val underlying: Vector[ContractId]) extends AnyVal {
    def asCidOption: Option[ContractId] = underlying match {
      case Vector() => None
      case Vector(v) => Some(v)
      case _ => throw new IllegalArgumentException(s"Expected a contractId vector of size 0 or 1, got ${underlying.size}")
    }
  }

  implicit class CidOptionOps(val underlying: Option[ContractId]) extends AnyVal {
    def asCidVector: Vector[ContractId] = underlying match {
      case None => Vector.empty
      case Some(v) => Vector(v)
    }
  }

  implicit class KeyMapVectorOps(val underlying: Map[GlobalKey, Vector[ContractId]]) extends AnyVal {
    def asCidOptionMap: Map[GlobalKey, Option[ContractId]] =
      underlying.transform((_, v) => v.asCidOption)
  }

  implicit class KeyMapOptionOps(val underlying: Map[GlobalKey, Option[ContractId]]) extends AnyVal {
    def asCidVectorMap: Map[GlobalKey, Vector[ContractId]] =
      underlying.transform((_, v) => v.asCidVector)
  }

}
