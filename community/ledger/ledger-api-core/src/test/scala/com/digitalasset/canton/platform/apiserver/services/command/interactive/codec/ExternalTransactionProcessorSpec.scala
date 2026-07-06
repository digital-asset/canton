// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive.codec

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset
import com.digitalasset.canton.data.LedgerTimeBoundaries
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.ledger.api.{CommandId, Commands}
import com.digitalasset.canton.ledger.participant.state.index.ContractStore
import com.digitalasset.canton.ledger.participant.state.{
  RoutingSynchronizerState,
  SubmitterInfo,
  SyncService,
  SynchronizerRank,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandExecutionResult,
  CommandInterpretationResult,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LedgerUserId, LfTimestamp}
import com.digitalasset.daml.lf.command.ApiCommands
import com.digitalasset.daml.lf.crypto.Hash as LfHash
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{
  Node,
  NodeId,
  SerializationVersion as LfSerializationVersion,
  SubmittedTransaction,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class ExternalTransactionProcessorSpec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with HasExecutionContext
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private def commandExecutionResultFor(
      transaction: VersionedTransaction,
      protocolVersion: ProtocolVersion,
      submissionSeed: String,
      nodeSeeds: ImmArray[(NodeId, LfHash)],
  ): (CommandExecutionResult, Commands) = {
    val physicalSynchronizerId =
      DefaultTestIdentities.physicalSynchronizerId.copy(protocolVersion = protocolVersion)
    val commandId = Ref.CommandId.assertFromString("command")
    val submitterInfo = SubmitterInfo(
      actAs = List.empty,
      readAs = List.empty,
      userId = LedgerUserId.assertFromString("app"),
      commandId = commandId,
      deduplicationPeriod = DeduplicationOffset(None),
      submissionId = None,
      externallySignedSubmission = None,
      // transactionHash is computed later from the verified signature during execution
      // (see EnrichedTransactionData / ExternalTransactionProcessor); unknown at this point.
      transactionHash = None,
    )
    val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = LfTimestamp.Epoch,
      workflowId = None,
      preparationTime = LfTimestamp.Epoch,
      submissionSeed = LfHash.hashPrivateKey(submissionSeed),
      timeBoundaries = LedgerTimeBoundaries.unconstrained,
      optUsedPackages = None,
      optNodeSeeds = Some(nodeSeeds),
      optByKeyNodes = None,
    )
    val commandExecutionResult = CommandExecutionResult(
      commandInterpretationResult = CommandInterpretationResult(
        submitterInfo = submitterInfo,
        optSynchronizerId = None,
        transactionMeta = transactionMeta,
        transaction = SubmittedTransaction(transaction),
        dependsOnLedgerTime = false,
        interpretationTimeNanos = 0L,
        processedDisclosedContracts = ImmArray.Empty,
      ),
      synchronizerRank = SynchronizerRank.single(physicalSynchronizerId),
      routingSynchronizerState = mock[RoutingSynchronizerState],
    )
    val commands = Commands(
      workflowId = None,
      userId = submitterInfo.userId,
      commandId = CommandId(commandId),
      submissionId = None,
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = LfTimestamp.Epoch,
      deduplicationPeriod = DeduplicationOffset(None),
      commands = ApiCommands(
        commands = ImmArray.Empty,
        ledgerEffectiveTime = LfTimestamp.Epoch,
        commandsReference = "command",
      ),
      disclosedContracts = ImmArray.Empty,
      synchronizerId = None,
      prefetchKeys = Seq.empty,
      tapsMaxPasses = None,
    )
    (commandExecutionResult, commands)
  }

  private def processorFor(transaction: VersionedTransaction): ExternalTransactionProcessor = {
    val enricher = mock[InteractiveSubmissionEnricher]
    when(
      enricher.enrichVersionedTransaction(any[VersionedTransaction])(
        any[ExecutionContext],
        anyTraceContext,
      )
    ).thenReturn(FutureUnlessShutdown.pure(transaction))
    new ExternalTransactionProcessor(
      enricher = enricher,
      contractStore = mock[ContractStore],
      syncService = mock[SyncService],
      config = InteractiveSubmissionServiceConfig.Default,
      loggerFactory = loggerFactory,
    )
  }

  private def vDevCreateTransaction(seed: String): (NodeId, VersionedTransaction) = {
    val nodeId = NodeId(0)
    val signatory = Ref.Party.assertFromString("Alice")
    val createNode = Node.Create(
      coid = Value.ContractId.V1(LfHash.hashPrivateKey(seed)),
      packageName = Ref.PackageName.assertFromString("pkg"),
      templateId = Ref.Identifier.assertFromString("pkgid:Mod:Template"),
      arg = Value.ValueUnit,
      signatories = Set(signatory),
      stakeholders = Set(signatory),
      keyOpt = None,
      version = LfSerializationVersion.VDev,
    )
    val transaction = VersionedTransaction(
      LfSerializationVersion.VDev,
      Map(nodeId -> createNode),
      ImmArray(nodeId),
    )
    (nodeId, transaction)
  }

  private def prepare(
      transaction: VersionedTransaction,
      protocolVersion: ProtocolVersion,
      submissionSeed: String,
      hashingSchemeVersion: HashingSchemeVersion,
      nodeSeeds: ImmArray[(NodeId, LfHash)] = ImmArray.Empty,
  ) = {
    val (commandExecutionResult, commands) =
      commandExecutionResultFor(transaction, protocolVersion, submissionSeed, nodeSeeds)
    val processor = processorFor(transaction)
    processor.processPrepare(
      commandExecutionResult,
      commands,
      PositiveInt.one,
      HashTracer.NoOp,
      maxRecordTime = None,
      hashingSchemeVersion = hashingSchemeVersion,
    )
  }

  "ExternalTransactionProcessor" should {
    "honor the requested hashing scheme for non-dev prepared transactions" in {
      val transaction = VersionedTransaction(
        LfSerializationVersion.V2,
        Map.empty,
        ImmArray.Empty,
      )
      val result = prepare(
        transaction,
        ProtocolVersion.v35,
        "prepared-requested-hash-version-test",
        HashingSchemeVersion.V3,
      ).valueOrFailShutdown("prepare transaction").futureValue

      result.hashVersion shouldBe HashingSchemeVersion.V3
    }

    "reject VDev prepared transactions when the requested hashing scheme is V2" in {
      val (nodeId, transaction) = vDevCreateTransaction("prepared-vdev-requested-v2-contract")

      val result = prepare(
        transaction,
        ProtocolVersion.dev,
        "prepared-vdev-requested-v2-test",
        HashingSchemeVersion.V2,
        ImmArray(nodeId -> LfHash.hashPrivateKey("prepared-vdev-requested-v2-node")),
      ).value.failOnShutdown("prepare transaction").futureValue

      result.left.value.reason shouldBe
        "Cannot hash node with LF serialization version VDev using hashing scheme V2." +
        " Please use hashing scheme V4 or higher."
    }

    "honor the requested hashing scheme for dev prepared transactions" in {
      val (nodeId, transaction) = vDevCreateTransaction("prepared-vdev-requested-v4-contract")

      val result = prepare(
        transaction,
        ProtocolVersion.dev,
        "prepared-vdev-requested-v4-test",
        HashingSchemeVersion.V4,
        ImmArray(nodeId -> LfHash.hashPrivateKey("prepared-vdev-requested-v4-node")),
      ).valueOrFailShutdown("prepare transaction").futureValue

      result.hashVersion shouldBe HashingSchemeVersion.V4
    }

    "reject prepared transactions when the requested hashing scheme is V4 and the protocol version is stable" in {
      val transaction = VersionedTransaction(
        LfSerializationVersion.V2,
        Map.empty,
        ImmArray.Empty,
      )
      val result = prepare(
        transaction,
        ProtocolVersion.v35,
        "prepared-pv35-requested-v4-test",
        HashingSchemeVersion.V4,
      ).value.failOnShutdown("prepare transaction").futureValue

      result.left.value.reason shouldBe
        "Hashing scheme version V4 is not supported on protocol version 35." +
        " Minimum protocol version for hashing version V4: dev." +
        " Supported hashing version on protocol version 35: V2, V3"
    }
  }
}
