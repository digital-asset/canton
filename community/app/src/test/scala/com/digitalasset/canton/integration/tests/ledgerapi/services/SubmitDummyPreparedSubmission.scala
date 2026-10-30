// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  InteractiveSubmissionServiceGrpc,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallWithMainActorAuthTests
import com.digitalasset.canton.serialization.ProtoConverter
import io.scalaland.chimney.dsl.*

import java.util.UUID
import scala.concurrent.Future

trait SubmitDummyPreparedSubmission extends SubmitDummyCommand {
  self: ServiceCallWithMainActorAuthTests =>

  protected def dummyPrepareSubmissionRequest(
      party: String,
      userId: String,
      accountId: String = "",
  ): PrepareSubmissionRequest =
    PrepareSubmissionRequest(
      userId = userId,
      commandId = UUID.randomUUID().toString,
      commands = List(
        createWithOperator(templateIds.dummy, party)
      ),
      minLedgerTime = None,
      actAs = Seq(party),
      readAs = Seq.empty,
      disclosedContracts = Seq.empty,
      synchronizerId = "",
      packageIdSelectionPreference = Seq.empty,
      verboseHashing = true,
      prefetchContractKeys = Seq.empty,
      maxRecordTime = Option.empty,
      estimateTrafficCost = None,
      tapsMaxPasses = None,
      hashingSchemeVersion = None,
      accountId = accountId,
    )

  protected def dummyExecuteSubmissionRequest(
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
      accountId: String = "",
  ): ExecuteSubmissionRequest =
    ExecuteSubmissionRequest(
      preparedTransaction = preparedSubmission.preparedTransaction,
      partySignatures = None,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(
        ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
      ),
      submissionId = UUID.randomUUID().toString,
      userId = userId,
      hashingSchemeVersion = preparedSubmission.hashingSchemeVersion,
      minLedgerTime = None,
      accountId = accountId,
    )

  protected def prepareSubmission(
      token: Option[String],
      party: String,
      userId: String,
      accountId: String = "",
  ): Future[PrepareSubmissionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .prepareSubmission(dummyPrepareSubmissionRequest(party, userId, accountId))

  protected def executeSubmission(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
      accountId: String = "",
  ): Future[ExecuteSubmissionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmission(dummyExecuteSubmissionRequest(userId, preparedSubmission, accountId))

  protected def executeSubmissionAndWait(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
      accountId: String = "",
  ): Future[ExecuteSubmissionAndWaitResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmissionAndWait(
        dummyExecuteSubmissionRequest(userId, preparedSubmission, accountId)
          .transformInto[ExecuteSubmissionAndWaitRequest]
      )

  protected def executeSubmissionAndWaitForTransaction(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
      accountId: String = "",
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmissionAndWaitForTransaction(
        dummyExecuteSubmissionRequest(userId, preparedSubmission, accountId)
          .into[ExecuteSubmissionAndWaitForTransactionRequest]
          .withFieldConst(_.transactionFormat, None)
          .transform
      )

}
