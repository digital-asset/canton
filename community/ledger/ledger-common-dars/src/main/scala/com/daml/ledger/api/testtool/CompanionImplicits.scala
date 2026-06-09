// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.experimental.exceptions.ExceptionTester
import com.daml.ledger.test.java.model.iou.Iou
import com.daml.ledger.test.java.model.test.{
  Agreement,
  AgreementFactory,
  CallablePayout,
  Delegated,
  Delegation,
  DiscloseCreate,
  Divulgence1,
  Divulgence2,
  Dummy,
  DummyFactory,
  DummyWithParam,
  TriProposal,
  WithObservers,
  Witnesses as TestWitnesses,
}
import com.daml.ledger.test.java.model.trailingnones.TrailingNones
import com.daml.ledger.test.java.semantic.divulgencetests.{
  Contract,
  DivulgenceProposal,
  DummyFlexibleController,
}
import com.daml.ledger.test.java.semantic.{divulgencetests, semantictests}

class CompanionImplicits(testDars: TestDars) {
  private val modelTestsPackageId = testDars.ModelTestDar.packageId
  private val semanticTestsPackageId = testDars.SemanticTestDar.packageId
  private val experimentalTestsPackageId = testDars.ExperimentalTestDar.packageId

  // Codegen is produced on the latest LF-version DARs.
  // To support running LAPITT on different LF version, we retrieve the package ID from the
  // respective DAR and update the companion.
  implicit val dummyCompanion
      : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] =
    Dummy.COMPANION.withPackageId(modelTestsPackageId)
  implicit val dummyWithParamCompanion: ContractCompanion.WithoutKey[
    DummyWithParam.Contract,
    DummyWithParam.ContractId,
    DummyWithParam,
  ] = DummyWithParam.COMPANION.withPackageId(modelTestsPackageId)
  implicit val dummyFactoryCompanion
      : ContractCompanion.WithoutKey[DummyFactory.Contract, DummyFactory.ContractId, DummyFactory] =
    DummyFactory.COMPANION.withPackageId(modelTestsPackageId)
  implicit val withObserversCompanion: ContractCompanion.WithoutKey[
    WithObservers.Contract,
    WithObservers.ContractId,
    WithObservers,
  ] = WithObservers.COMPANION.withPackageId(modelTestsPackageId)
  implicit val callablePayoutCompanion: ContractCompanion.WithoutKey[
    CallablePayout.Contract,
    CallablePayout.ContractId,
    CallablePayout,
  ] = CallablePayout.COMPANION.withPackageId(modelTestsPackageId)
  implicit val delegatedCompanion: ContractCompanion.WithoutKey[
    Delegated.Contract,
    Delegated.ContractId,
    Delegated,
  ] = Delegated.COMPANION.withPackageId(modelTestsPackageId)
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION.withPackageId(modelTestsPackageId)
  implicit val discloseCreatedCompanion: ContractCompanion.WithoutKey[
    DiscloseCreate.Contract,
    DiscloseCreate.ContractId,
    DiscloseCreate,
  ] = DiscloseCreate.COMPANION.withPackageId(modelTestsPackageId)
  implicit val testWitnessesCompanion: ContractCompanion.WithoutKey[
    TestWitnesses.Contract,
    TestWitnesses.ContractId,
    TestWitnesses,
  ] = TestWitnesses.COMPANION.withPackageId(modelTestsPackageId)
  implicit val divulgence1Companion
      : ContractCompanion.WithoutKey[Divulgence1.Contract, Divulgence1.ContractId, Divulgence1] =
    Divulgence1.COMPANION.withPackageId(modelTestsPackageId)
  implicit val divulgence2Companion
      : ContractCompanion.WithoutKey[Divulgence2.Contract, Divulgence2.ContractId, Divulgence2] =
    Divulgence2.COMPANION.withPackageId(modelTestsPackageId)
  implicit val iouCompanion: ContractCompanion.WithoutKey[Iou.Contract, Iou.ContractId, Iou] =
    Iou.COMPANION.withPackageId(modelTestsPackageId)
  implicit val agreementFactoryCompanion: ContractCompanion.WithoutKey[
    AgreementFactory.Contract,
    AgreementFactory.ContractId,
    AgreementFactory,
  ] = AgreementFactory.COMPANION.withPackageId(modelTestsPackageId)
  implicit val agreementCompanion
      : ContractCompanion.WithoutKey[Agreement.Contract, Agreement.ContractId, Agreement] =
    Agreement.COMPANION.withPackageId(modelTestsPackageId)

  implicit val triProposalCompanion
      : ContractCompanion.WithoutKey[TriProposal.Contract, TriProposal.ContractId, TriProposal] =
    TriProposal.COMPANION.withPackageId(modelTestsPackageId)

  implicit val trailingNonesCompanion: ContractCompanion.WithoutKey[
    TrailingNones.Contract,
    TrailingNones.ContractId,
    TrailingNones,
  ] =
    TrailingNones.COMPANION.withPackageId(modelTestsPackageId)

  implicit val semanticTestsDummyCompanion: ContractCompanion.WithoutKey[
    divulgencetests.Dummy.Contract,
    divulgencetests.Dummy.ContractId,
    divulgencetests.Dummy,
  ] = divulgencetests.Dummy.COMPANION.withPackageId(semanticTestsPackageId)
  implicit val divulgeIouByExerciseCompanion: ContractCompanion.WithoutKey[
    DummyFlexibleController.Contract,
    DummyFlexibleController.ContractId,
    DummyFlexibleController,
  ] = DummyFlexibleController.COMPANION.withPackageId(semanticTestsPackageId)
  implicit val semanticTestsIouCompanion: ContractCompanion.WithoutKey[
    semantictests.Iou.Contract,
    semantictests.Iou.ContractId,
    semantictests.Iou,
  ] = semantictests.Iou.COMPANION.withPackageId(semanticTestsPackageId)
  implicit val divulgenceProposalCompanion: ContractCompanion.WithoutKey[
    DivulgenceProposal.Contract,
    DivulgenceProposal.ContractId,
    DivulgenceProposal,
  ] = DivulgenceProposal.COMPANION.withPackageId(semanticTestsPackageId)
  implicit val contractCompanion
      : ContractCompanion.WithoutKey[Contract.Contract$, Contract.ContractId, Contract] =
    Contract.COMPANION.withPackageId(semanticTestsPackageId)

  implicit val exceptionTesterCompanion: ContractCompanion.WithoutKey[
    ExceptionTester.Contract,
    ExceptionTester.ContractId,
    ExceptionTester,
  ] = ExceptionTester.COMPANION.withPackageId(experimentalTestsPackageId)
}
