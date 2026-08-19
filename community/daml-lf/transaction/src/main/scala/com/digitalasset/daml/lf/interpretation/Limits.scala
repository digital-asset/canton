// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

final case class Limits(
    contractSignatories: Int,
    contractStakeholders: Int,
    keyMaintainers: Int,
    valueSize: Int,
    actingParties: Int,
    choiceObservers: Int,
    choiceAuthorizers: Int,
    externalCallResults: Int,
    externalCallResultSize: Int,
    nodeChildren: Int,
    queryResult: Int,
    transactionInputContracts: Int,
    transactionRoots: Int,
    transactionNodes: Int,
    totalInformees: Long,
)

object Limits {
  val Lenient = Limits(
    contractSignatories = Int.MaxValue,
    contractStakeholders = Int.MaxValue,
    keyMaintainers = Int.MaxValue,
    valueSize = Int.MaxValue,
    actingParties = Int.MaxValue,
    choiceObservers = Int.MaxValue,
    choiceAuthorizers = Int.MaxValue,
    externalCallResults = Int.MaxValue,
    externalCallResultSize = Int.MaxValue,
    nodeChildren = Int.MaxValue,
    queryResult = Int.MaxValue,
    transactionInputContracts = Int.MaxValue,
    transactionRoots = Int.MaxValue,
    transactionNodes = Int.MaxValue,
    totalInformees = Long.MaxValue,
  )
}
