// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.Id
import cats.syntax.option._
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.participant.state.v2.Update.PublicPackageUpload
import com.daml.ledger.participant.state.v2._
import com.daml.lf.CantonOnly.LfVersionedTransaction
import com.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  LedgerTransactionNodeStatistics,
  LfCommittedTransaction,
  LfHash,
  LfTransaction,
  LfTransactionVersion,
}

/** Default values for objects from the Daml repo for unit testing */
object DefaultDamlValues {
  def lfApplicationId(index: Int = 0): Ref.ApplicationId =
    Ref.ApplicationId.assertFromString(s"application-id-$index")
  def applicationId(index: Int = 0): ApplicationId = ApplicationId(lfApplicationId(index))

  def lfCommandId(index: Int = 0): Ref.CommandId =
    Ref.CommandId.assertFromString(s"command-id-$index")
  def commandId(index: Int = 0): CommandId = CommandId(lfCommandId(index))

  def submissionId(index: Int = 0): LedgerSubmissionId =
    LedgerSubmissionId.assertFromString(s"submission-id-$index")

  lazy val deduplicationDuration: DeduplicationDuration = DeduplicationDuration(
    java.time.Duration.ofSeconds(100)
  )

  lazy val ledgerConfiguration: LedgerConfiguration = LedgerConfiguration(
    generation = 1L,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationDuration = java.time.Duration.ofDays(1L),
  )

  def lfTransactionId(index: Int): Ref.TransactionId =
    Ref.TransactionId.assertFromString(s"lf-transaction-id-$index")

  def submitterInfo(
      actAs: List[LfPartyId],
      applicationId: ApplicationId = DefaultDamlValues.applicationId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      deduplicationPeriod: DeduplicationPeriod = deduplicationDuration,
      submissionId: Option[LedgerSubmissionId] = DefaultDamlValues.submissionId().some,
      ledgerConfiguration: LedgerConfiguration = DefaultDamlValues.ledgerConfiguration,
  ): SubmitterInfo =
    SubmitterInfo(
      actAs,
      List.empty, // readAs parties in submitter info are ignored by canton
      applicationId.unwrap,
      commandId.unwrap,
      deduplicationPeriod,
      submissionId,
      ledgerConfiguration,
    )

  def changeId(
      actAs: Set[LfPartyId],
      applicationId: ApplicationId = DefaultDamlValues.applicationId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
  ): ChangeId =
    ChangeId(applicationId.unwrap, commandId.unwrap, actAs)

  def completionInfo(
      actAs: List[LfPartyId],
      applicationId: ApplicationId = DefaultDamlValues.applicationId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      optDeduplicationPeriod: Option[DeduplicationPeriod] = Some(deduplicationDuration),
      submissionId: Option[LedgerSubmissionId] = DefaultDamlValues.submissionId().some,
      statistics: Option[LedgerTransactionNodeStatistics] = None,
  ): CompletionInfo =
    CompletionInfo(
      actAs,
      applicationId.unwrap,
      commandId.unwrap,
      optDeduplicationPeriod,
      submissionId,
      statistics,
    )

  def lfhash(index: Int = 0): LfHash = {
    val bytes = new Array[Byte](32)
    for (i <- 0 to 3) {
      bytes(i) = (index >>> (24 - i * 8)).toByte
    }
    LfHash.assertFromByteArray(bytes)
  }

  def transactionMeta(
      ledgerEffectiveTime: CantonTimestamp = CantonTimestamp.Epoch,
      workflowId: Option[WorkflowId] = None,
      submissionTime: CantonTimestamp = CantonTimestamp.Epoch,
      submissionSeed: LfHash = lfhash(),
  ): TransactionMeta =
    TransactionMeta(
      ledgerEffectiveTime.toLf,
      workflowId.map(_.unwrap),
      submissionTime.toLf,
      submissionSeed,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

  lazy val emptyTransaction: LfTransaction =
    LfTransaction(nodes = Map.empty, roots = ImmArray.empty)
  lazy val emptyVersionedTransaction: LfVersionedTransaction =
    LfVersionedTransaction(LfTransactionVersion.VDev, Map.empty, ImmArray.empty)
  lazy val emptyCommittedTransaction: LfCommittedTransaction =
    LfCommittedTransaction.subst[Id](emptyVersionedTransaction)

  def dummyStateUpdate(
      timestamp: CantonTimestamp = CantonTimestamp.Epoch
  ): com.daml.ledger.participant.state.v2.Update =
    PublicPackageUpload(List.empty, None, timestamp.toLf, None)
}
