// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.TestDars
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.keys.contractidtests.{Contract, ContractRef}
import com.daml.ledger.test.java.keys.da.types
import com.daml.ledger.test.java.keys.test.{
  Delegated,
  Delegation,
  LocalKeyVisibilityOperations,
  MaintainerNotSignatory,
  ShowDelegated,
  TextKey,
  TextKeyOperations,
  WithKey,
}
import com.daml.ledger.test.java.model.test.{CallablePayout, Dummy}

class ContractKeysCompanionImplicits(testDars: TestDars) {

  implicit val dummyCompanion
      : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] =
    Dummy.COMPANION.withPackageId(testDars.ModelTestDar.packageId)
  implicit val textKeyCompanion: ContractCompanion.WithKey[
    TextKey.Contract,
    TextKey.ContractId,
    TextKey,
    types.Tuple2[String, String],
  ] = TextKey.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val textKeyOperationsCompanion: ContractCompanion.WithoutKey[
    TextKeyOperations.Contract,
    TextKeyOperations.ContractId,
    TextKeyOperations,
  ] = TextKeyOperations.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val callablePayoutCompanion: ContractCompanion.WithoutKey[
    CallablePayout.Contract,
    CallablePayout.ContractId,
    CallablePayout,
  ] = CallablePayout.COMPANION.withPackageId(testDars.ModelTestDar.packageId)
  implicit val delegatedCompanion: ContractCompanion.WithKey[
    Delegated.Contract,
    Delegated.ContractId,
    Delegated,
    types.Tuple2[String, String],
  ] = Delegated.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val showDelegatedCompanion: ContractCompanion.WithoutKey[
    ShowDelegated.Contract,
    ShowDelegated.ContractId,
    ShowDelegated,
  ] = ShowDelegated.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val maintainerNotSignatoryCompanion: ContractCompanion.WithKey[
    MaintainerNotSignatory.Contract,
    MaintainerNotSignatory.ContractId,
    MaintainerNotSignatory,
    String,
  ] = MaintainerNotSignatory.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val localKeyVisibilityOperationsCompanion: ContractCompanion.WithoutKey[
    LocalKeyVisibilityOperations.Contract,
    LocalKeyVisibilityOperations.ContractId,
    LocalKeyVisibilityOperations,
  ] = LocalKeyVisibilityOperations.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val withKeyCompanion: ContractCompanion.WithKey[
    WithKey.Contract,
    WithKey.ContractId,
    WithKey,
    String,
  ] = WithKey.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
  implicit val contractCompanion
      : ContractCompanion.WithoutKey[Contract.Contract$, Contract.ContractId, Contract] =
    Contract.COMPANION
  implicit val contractRefCompanion: ContractCompanion.WithKey[
    ContractRef.Contract,
    ContractRef.ContractId,
    ContractRef,
    String,
  ] = ContractRef.COMPANION.withPackageId(testDars.KeysTestDar.packageId)
}
