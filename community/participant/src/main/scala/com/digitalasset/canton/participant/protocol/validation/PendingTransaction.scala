// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.protocol.{LfContractId, RequestId, TransactionId}
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.{RequestCounter, SequencerCounter, WorkflowId}

/** Storing metadata of pending transactions required for emitting transactions on the sync API. */
final case class PendingTransaction(
    txId: TransactionId,
    modelConformanceResultE: Either[
      ModelConformanceChecker.ErrorWithSubviewsCheck,
      ModelConformanceChecker.Result,
    ],
    workflowIdO: Option[WorkflowId],
    requestTime: CantonTimestamp,
    requestCounter: RequestCounter,
    requestSequencerCounter: SequencerCounter,
    transactionValidationResult: TransactionValidationResult,
    mediatorId: MediatorId,
) extends PendingRequestData {

  val requestId: RequestId = RequestId(requestTime)

  override def pendingContracts: Set[LfContractId] =
    transactionValidationResult.createdContracts.keySet
}
