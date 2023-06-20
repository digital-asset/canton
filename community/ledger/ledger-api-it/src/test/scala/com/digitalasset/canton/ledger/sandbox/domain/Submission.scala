// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox.domain

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.logging.LoggingContextWithTrace

private[sandbox] sealed trait Submission extends Product with Serializable {
  def submissionId: Ref.SubmissionId
  def loggingContext: LoggingContextWithTrace
}

private[sandbox] object Submission {
  final case class Transaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit val loggingContext: LoggingContextWithTrace)
      extends Submission {
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    val submissionId: SubmissionId = {
      // If we were to make SoX production-ready we would make the submissionId non-optional.
      // .get deemed safe since no transaction submission should have the submission id empty
      submitterInfo.submissionId.get
    }
  }

  final case class Config(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit val loggingContext: LoggingContextWithTrace)
      extends Submission
  final case class AllocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit val loggingContext: LoggingContextWithTrace)
      extends Submission

  final case class UploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit val loggingContext: LoggingContextWithTrace)
      extends Submission
}
