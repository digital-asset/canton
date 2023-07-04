// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction.*
import com.daml.lf.value.Value.{ContractId, ValueNone}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.ledger.sandbox.bridge.LedgerBridge
import com.digitalasset.canton.ledger.sandbox.domain.Submission
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BridgeWriteServiceTest extends AnyFlatSpec with MockitoSugar with Matchers {
  private val nodeId = NodeId(0)
  private val contractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("c0"), Bytes.assertFromString("00"))

  private val node = Node.Create(
    contractId,
    templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
    arg = ValueNone,
    agreementText = "dummyAgreement",
    signatories = Set.empty,
    stakeholders = Set.empty,
    keyOpt = None,
    version = TransactionVersion.minVersion,
  )

  private val tx = SubmittedTransaction(
    VersionedTransaction(TransactionVersion.VDev, Map(nodeId -> node), ImmArray(nodeId))
  )

  private val submitterInfo = SubmitterInfo(
    actAs = List.empty,
    readAs = List.empty,
    applicationId = Ref.ApplicationId.assertFromString("a0"),
    commandId = Ref.CommandId.assertFromString("c0"),
    deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(Offset.beforeBegin),
    submissionId = Some(Ref.SubmissionId.assertFromString("some-submission-id")),
    ledgerConfiguration = Configuration.reasonableInitialConfiguration,
  )

  private val transactionMeta = TransactionMeta(
    ledgerEffectiveTime = Time.Timestamp.now(),
    workflowId = None,
    submissionTime = Time.Timestamp.now(),
    submissionSeed = crypto.Hash.hashPrivateKey("k0"),
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
    optDomainId = None,
  )

  private val submission = Submission.Transaction(
    submitterInfo,
    transactionMeta,
    transaction = tx,
    estimatedInterpretationCost = 0,
    processedDisclosedContracts = ImmArray.empty,
  )(LoggingContextWithTrace.ForTesting)

  "Success Mapper" should "add transaction statistics" in {
    val expected = TransactionNodeStatistics(tx)

    val update =
      LedgerBridge.transactionAccepted(
        transactionSubmission = submission,
        index = 0,
        currentTimestamp = Time.Timestamp.now(),
      )
    update.completionInfoO.flatMap(_.statistics) shouldBe Some(expected)
  }

  "toTransactionAccepted" should "forward populate contract metadata" in {
    val update =
      LedgerBridge.transactionAccepted(
        transactionSubmission = submission,
        index = 0,
        currentTimestamp = Time.Timestamp.now(),
      )

    update.contractMetadata shouldBe Map(
      contractId -> Bytes.fromHexString(contractId.coid)
    )
  }
}
