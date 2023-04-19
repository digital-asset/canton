// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.command.{ApiCommands as LfCommands, DisclosedContract}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{Identifier, ParticipantId}
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, ResultDone}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  ProcessedDisclosedContract,
  SubmittedTransaction,
  Transaction,
  TransactionVersion,
}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.domain.{CommandId, Commands, LedgerId}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractStore,
  IndexPackagesService,
}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration

class StoreBackedCommandExecutorSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val processedDisclosedContracts = ImmArray(
    ProcessedDisclosedContract(
      templateId = Identifier.assertFromString("some:pkg:identifier"),
      contractId = TransactionBuilder.newCid,
      argument = Value.ValueNil,
      createdAt = Time.Timestamp.Epoch,
      driverMetadata = Bytes.Empty,
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      agreementText = "some agreement text",
      version = TransactionVersion.V15,
    )
  )

  private val emptyTransactionMetadata = Transaction.Metadata(
    submissionSeed = None,
    submissionTime = Time.Timestamp.now(),
    usedPackages = Set.empty,
    dependsOnTime = false,
    nodeSeeds = ImmArray.Empty,
    globalKeyMapping = Map.empty,
    processedDisclosedContracts = processedDisclosedContracts,
  )

  "execute" should {
    "add interpretation time and used disclosed contracts to result" in {
      val mockEngine = mock[Engine]
      when(
        mockEngine.submit(
          submitters = any[Set[Ref.Party]],
          readAs = any[Set[Ref.Party]],
          cmds = any[com.daml.lf.command.ApiCommands],
          participantId = any[ParticipantId],
          submissionSeed = any[Hash],
          disclosures = any[ImmArray[DisclosedContract]],
        )(any[LoggingContext])
      )
        .thenReturn(
          ResultDone[(SubmittedTransaction, Transaction.Metadata)](
            (TransactionBuilder.EmptySubmitted, emptyTransactionMetadata)
          )
        )

      val commands = Commands(
        ledgerId = Some(LedgerId("ledgerId")),
        workflowId = None,
        applicationId = Ref.ApplicationId.assertFromString("applicationId"),
        commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
        submissionId = None,
        actAs = Set.empty,
        readAs = Set.empty,
        submittedAt = Time.Timestamp.Epoch,
        deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
        commands = LfCommands(
          commands = ImmArray.Empty,
          ledgerEffectiveTime = Time.Timestamp.Epoch,
          commandsReference = "",
        ),
        disclosedContracts = ImmArray.empty,
      )
      val submissionSeed = Hash.hashPrivateKey("a key")
      val configuration = Configuration(
        generation = 1,
        timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ZERO,
          minSkew = Duration.ZERO,
          maxSkew = Duration.ZERO,
        ).get,
        maxDeduplicationDuration = Duration.ZERO,
      )

      val instance = new StoreBackedCommandExecutor(
        mockEngine,
        Ref.ParticipantId.assertFromString("anId"),
        mock[IndexPackagesService],
        mock[ContractStore],
        AuthorityResolver(),
        Metrics.ForTesting,
      )

      LoggingContext.newLoggingContext { implicit context =>
        instance.execute(commands, submissionSeed, configuration).map { actual =>
          actual.foreach { actualResult =>
            actualResult.interpretationTimeNanos should be > 0L
            actualResult.processedDisclosedContracts shouldBe processedDisclosedContracts
          }
          succeed
        }
      }
    }
  }
}