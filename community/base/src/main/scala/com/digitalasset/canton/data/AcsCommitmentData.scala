// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{Digest, LegacyCommitmentPeriod}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}

/** Super trait to both [[BufferedAcsCommitment]] and
  * [[com.digitalasset.canton.protocol.messages.LegacyAcsCommitment]].
  */
trait AcsCommitmentData extends PrettyPrinting {
  def sender: ParticipantId
  def counterParticipant: ParticipantId
  def period: LegacyCommitmentPeriod
  def commitment: Digest.HashedDigestType
}

/** Light version of the protocol message, i.e., abstracts the physical synchronizer id to a logical
  * one.
  *
  * We do not store commitment signatures here, and also not sufficient data to validate the
  * signature. To validate signatures, one must use the commitments stored in the commitments table,
  * which contain the protocol message (and, in particular, the physical synchronizer id).
  */
final case class BufferedAcsCommitment(
    synchronizerId: SynchronizerId,
    sender: ParticipantId,
    counterParticipant: ParticipantId,
    period: LegacyCommitmentPeriod,
    commitment: Digest.HashedDigestType,
) extends AcsCommitmentData {
  override lazy val pretty: Pretty[BufferedAcsCommitment] =
    prettyOfClass(
      param("synchronizerId", _.synchronizerId),
      param("sender", _.sender),
      param("counterParticipant", _.counterParticipant),
      param("period", _.period),
      param("commitment", _.commitment),
    )
}
