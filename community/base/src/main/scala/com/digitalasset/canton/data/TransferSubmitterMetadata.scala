// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Information about the submitters of the transaction
  * This data structure is similar to [[com.digitalasset.canton.data.SubmitterMetadata]]
  * Please switch to SubmitterMetadata if you need to add dedupPeriod to this case class.
  */
final case class TransferSubmitterMetadata(
    submitter: LfPartyId,
    applicationId: LedgerApplicationId,
    submittingParticipant: LedgerParticipantId,
    commandId: LedgerCommandId,
    submissionId: Option[LedgerSubmissionId],
) extends PrettyPrinting {

  override def pretty: Pretty[TransferSubmitterMetadata] = prettyOfClass(
    param("submitter", _.submitter),
    param("application id", _.applicationId),
    param("submitter participant", _.submittingParticipant),
    param("command id", _.commandId),
    paramIfDefined("submission id", _.submissionId),
  )
}
