// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{LfPartyId, WorkflowId}

final case class TransactionValidationResult(
    transactionId: TransactionId,
    submitterMetadataO: Option[SubmitterMetadata],
    workflowIdO: Option[WorkflowId],
    contractConsistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
    authenticationResult: Map[ViewPosition, AuthenticationError],
    authorizationResult: Map[ViewPosition, String],
    modelConformanceResultET: EitherT[
      FutureUnlessShutdown,
      ModelConformanceChecker.ErrorWithSubTransaction,
      ModelConformanceChecker.Result,
    ],
    internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
    consumedInputsOfHostedParties: Map[LfContractId, Set[LfPartyId]],
    witnessed: Map[LfContractId, GenContractInstance],
    createdContracts: Map[LfContractId, NewContractInstance],
    transient: Map[LfContractId, Set[LfPartyId]],
    activenessResult: ActivenessResult,
    viewValidationResults: Map[ViewPosition, ViewValidationResult],
    timeValidationResultE: Either[TimeCheckFailure, Unit],
    hostedWitnesses: Set[LfPartyId],
    replayCheckResult: Option[String],
    validatedExternalTransactionHash: Option[Hash],
) {

  def commitSet(
      requestId: RequestId
  )(implicit loggingContext: ErrorLoggingContext): CommitSet =
    CommitSet.createForTransaction(
      activenessResult,
      requestId,
      consumedInputsOfHostedParties,
      transient,
      createdContracts,
    )
}
