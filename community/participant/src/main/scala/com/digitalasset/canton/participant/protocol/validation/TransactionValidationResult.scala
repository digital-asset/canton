// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.functor.*
import com.digitalasset.canton.data.SubmitterMetadata
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, TransferCounter, WorkflowId}

final case class TransactionValidationResult(
    transactionId: TransactionId,
    confirmationPolicy: ConfirmationPolicy,
    submitterMetadataO: Option[SubmitterMetadata],
    workflowIdO: Option[WorkflowId],
    contractConsistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
    authenticationResult: Map[ViewHash, String],
    authorizationResult: Map[ViewHash, String],
    modelConformanceResultE: Either[
      ModelConformanceChecker.ErrorWithSubviewsCheck,
      ModelConformanceChecker.Result,
    ],
    internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
    consumedInputsOfHostedParties: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    createdContracts: Map[LfContractId, SerializableContract],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    keyUpdates: Map[LfGlobalKey, ContractKeyJournal.Status],
    successfulActivenessCheck: Boolean,
    viewValidationResults: Map[ViewHash, ViewValidationResult],
    timeValidationResultE: Either[TimeCheckFailure, Unit],
    hostedWitnesses: Set[LfPartyId],
) {

  def commitSet(
      requestId: RequestId
  )(protocolVersion: ProtocolVersion)(implicit loggingContext: ErrorLoggingContext): CommitSet = {
    if (successfulActivenessCheck) {
      val archivals = consumedInputsOfHostedParties ++ transient
      val transferCounter = TransferCounter.forCreatedContract(protocolVersion)
      val creations = createdContracts.fmap(c =>
        WithContractHash.fromContract(c, CommitSet.CreationCommit(c.metadata, transferCounter))
      )
      CommitSet(
        archivals = archivals,
        creations = creations,
        transferOuts = Map.empty,
        transferIns = Map.empty,
        keyUpdates = keyUpdates,
      )
    } else {
      SyncServiceAlarm
        .Warn(s"Request $requestId with failed activeness check is approved.")
        .report()
      // TODO(i12904) Handle this case gracefully
      throw new RuntimeException(s"Request $requestId with failed activeness check is approved.")
    }
  }
}
