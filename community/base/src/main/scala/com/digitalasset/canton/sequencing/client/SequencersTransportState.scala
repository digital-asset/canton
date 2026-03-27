// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.util.Mutex

import java.util.concurrent.atomic.AtomicReference

class SequencersTransportState(initialSequencerTransports: SequencerTransports) {
  private val lock = Mutex()

  private val sequencerTrustThreshold =
    new AtomicReference[PositiveInt](initialSequencerTransports.sequencerTrustThreshold)

  private val submissionRequestAmplification =
    new AtomicReference[SubmissionRequestAmplification](
      initialSequencerTransports.submissionRequestAmplification
    )

  def getSubmissionRequestAmplification: SubmissionRequestAmplification =
    submissionRequestAmplification.get()

  def changeTransport(
      sequencerTransports: SequencerTransports
  ): Unit =
    lock.exclusive {
      sequencerTrustThreshold.set(sequencerTransports.sequencerTrustThreshold)
      submissionRequestAmplification.set(sequencerTransports.submissionRequestAmplification)
    }
}
