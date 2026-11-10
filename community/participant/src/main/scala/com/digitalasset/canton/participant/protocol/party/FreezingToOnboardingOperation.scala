// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanion,
}

import scala.annotation.unused

/** Pending move of this participant's entry for a party from Freezing to Onboarding, recorded by
  * the party ACS import. It carries no data: the party and the synchronizer are its key, so several
  * imports for the same freeze record it once.
  */
final case class FreezingToOnboardingOperation()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      FreezingToOnboardingOperation.type
    ]
) extends HasProtocolVersionedWrapper[FreezingToOnboardingOperation] {
  @transient override protected lazy val companionObj: FreezingToOnboardingOperation.type =
    FreezingToOnboardingOperation

  private def toProtoV30: v30.FreezingToOnboardingOperation = v30.FreezingToOnboardingOperation()

  def toPendingOperation(
      synchronizerId: SynchronizerId,
      partyId: PartyId,
  ): PendingOperation[FreezingToOnboardingOperation, SynchronizerId] =
    PendingOperation(
      name = FreezingToOnboardingOperation.operationName,
      key = FreezingToOnboardingOperation.operationKey(partyId),
      operation = this,
      synchronizer = synchronizerId,
    )
}

object FreezingToOnboardingOperation extends VersioningCompanion[FreezingToOnboardingOperation] {
  override val name: String = "FreezingToOnboardingOperation"

  // Hosting states such as Freezing exist only from PV37 on
  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v37), v30.FreezingToOnboardingOperation)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      ),
  )

  def apply(releaseProtocolVersion: ReleaseProtocolVersion): FreezingToOnboardingOperation =
    FreezingToOnboardingOperation()(protocolVersionRepresentativeFor(releaseProtocolVersion.v))

  lazy val operationName: NonEmptyString =
    NonEmptyString.tryCreate("pending-freezing-to-onboarding")

  def operationKey(partyId: PartyId): String = partyId.toProtoPrimitive

  def partyIdFromKey(key: String): ParsingResult[PartyId] =
    PartyId.fromProtoPrimitive(key, "party_id")

  type PendingFreezingToOnboardingStore =
    PendingOperationStore[FreezingToOnboardingOperation, SynchronizerId]

  private def fromProtoV30(
      @unused _proto: v30.FreezingToOnboardingOperation
  ): ParsingResult[FreezingToOnboardingOperation] =
    protocolVersionRepresentativeFor(ProtoVersion(30)).map(FreezingToOnboardingOperation()(_))
}
