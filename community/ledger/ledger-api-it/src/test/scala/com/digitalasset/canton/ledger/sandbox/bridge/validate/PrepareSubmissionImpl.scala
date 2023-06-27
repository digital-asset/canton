// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox.bridge.validate

import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction as LfTransaction
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.ledger.sandbox.bridge.BridgeMetrics
import com.digitalasset.canton.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge.*
import com.digitalasset.canton.ledger.sandbox.domain.Rejection.*
import com.digitalasset.canton.ledger.sandbox.domain.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.{ExecutionContext, Future}

/** Precomputes the transaction effects for transaction submissions.
  * For other update types, this stage is a no-op.
  */
private[validate] class PrepareSubmissionImpl(
    bridgeMetrics: BridgeMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends PrepareSubmission
    with NamedLogging {

  override def apply(submission: Submission): AsyncValidation[PreparedSubmission] =
    submission match {
      case transactionSubmission @ Submission.Transaction(submitterInfo, _, transaction, _, _) =>
        Timed.future(
          bridgeMetrics.Stages.PrepareSubmission.timer,
          Future {
            transaction.transaction.contractKeyInputs
              .map(contractKeyInputs => {
                PreparedTransactionSubmission(
                  contractKeyInputs,
                  transaction.transaction.inputContracts,
                  transaction.transaction.updatedContractKeys,
                  transaction.transaction.consumedContracts,
                  Blinding.blind(transaction),
                  transaction.informees,
                  transactionSubmission,
                )
              })
              .left
              .map(
                withErrorLogger(submitterInfo.submissionId)(
                  invalidInputFromParticipantRejection(submitterInfo.toCompletionInfo())(_)
                )(transactionSubmission.loggingContext, logger)
              )
          },
        )
      case other => Future.successful(Right(NoOpPreparedSubmission(other)))
    }

  private def invalidInputFromParticipantRejection(completionInfo: CompletionInfo)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LfTransaction.KeyInputError => Rejection = {
    case Left(LfTransaction.InconsistentContractKey(key)) =>
      TransactionInternallyInconsistentKey(key, completionInfo)
    case Right(LfTransaction.DuplicateContractKey(key)) =>
      TransactionInternallyDuplicateKeys(key, completionInfo)
  }
}
