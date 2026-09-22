// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.topology.ParticipantId

trait HasSubmissionTrackerData extends PrettyPrintingFromCompanion {
  def submissionTrackerData: Option[SubmissionTrackerData]
}

/** submissionTrackerData extends HasSubmissionTrackerData because it's used as the
  * ViewSubmitterMetadata in the ViewTypeTest.
  */
final case class SubmissionTrackerData(
    submittingParticipant: ParticipantId,
    maxSequencingTime: CantonTimestamp,
) extends HasSubmissionTrackerData {
  override def submissionTrackerData: Option[SubmissionTrackerData] = Some(this)

  override def prettyCompanion: PrettyPrintingCompanion[SubmissionTrackerData] =
    SubmissionTrackerData
}

object SubmissionTrackerData extends PrettyPrintingCompanion[SubmissionTrackerData] {
  override protected val pretty: Pretty[SubmissionTrackerData] = prettyOfClass(
    param("submitting participant", _.submittingParticipant),
    param("max sequencing time", _.maxSequencingTime),
  )
}
