// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.functor._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.SubmitterMetadata
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.{LfPartyId, WorkflowId}

import scala.concurrent.Future

// TODO(Andreas, M40): Push model conformance down to individual views, not just for the transaction as a whole
case class TransactionValidationResult(
    transactionId: TransactionId,
    confirmationPolicies: NonEmpty[Set[ConfirmationPolicy]],
    submitterMetadata: Option[SubmitterMetadata],
    workflowId: Option[WorkflowId],
    contractConsistencyResult: Either[List[ReferenceToFutureContractError], Unit],
    modelConformanceResult: Either[ModelConformanceChecker.Error, ModelConformanceChecker.Result],
    consumedInputsOfHostedParties: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    createdContracts: Map[LfContractId, SerializableContract],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    keyUpdates: Map[LfGlobalKey, ContractKeyJournal.Status],
    successfulActivenessCheck: Boolean,
    viewValidationResults: Map[ViewHash, ViewValidationResult],
    timeValidationResult: Either[TimeCheckFailure, Unit],
    hostedInformeeStakeholders: Set[LfPartyId],
) {

  // TODO(#8744) avoid discarded future as part of AlarmStreamer design
  @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
  def commitSet(requestId: RequestId, alarm: String => Future[Unit]): CommitSet = {
    if (successfulActivenessCheck) {
      val archivals = consumedInputsOfHostedParties ++ transient
      val creations = createdContracts.fmap(c => WithContractHash.fromContract(c, c.metadata))
      CommitSet(
        archivals = archivals,
        creations = creations,
        transferOuts = Map.empty,
        transferIns = Map.empty,
        keyUpdates = keyUpdates,
      )
    } else {
      alarm(s"Request $requestId with failed activeness check is approved.")
      // TODO(M40) Handle this case gracefully
      throw new RuntimeException(s"Request $requestId with failed activeness check is approved.")
    }
  }
}
