// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

final case class OnboardingClearanceOperation(
    onboardingEffectiveAt: Option[EffectiveTime]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      OnboardingClearanceOperation.type
    ]
) extends HasProtocolVersionedWrapper[OnboardingClearanceOperation] {
  @transient override protected lazy val companionObj: OnboardingClearanceOperation.type =
    OnboardingClearanceOperation

  private def toProtoV30: v30.OnboardingClearanceOperation =
    v30.OnboardingClearanceOperation(onboardingEffectiveAt.map(_.toProtoPrimitive))

  def toPendingOperation(
      synchronizerId: SynchronizerId,
      partyId: PartyId,
  ): PendingOperation[OnboardingClearanceOperation, SynchronizerId] =
    PendingOperation(
      name = OnboardingClearanceOperation.operationName,
      key = OnboardingClearanceOperation.operationKey(partyId),
      operation = this,
      synchronizer = synchronizerId,
    )
}

object OnboardingClearanceOperation extends VersioningCompanion[OnboardingClearanceOperation] {
  override val name: String = "OnboardingClearanceOperation"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.OnboardingClearanceOperation)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  lazy val operationName: NonEmptyString =
    NonEmptyString.tryCreate("pending-onboarding-clearance")

  def operationKey(partyId: PartyId): String = partyId.toProtoPrimitive

  def partyIdFromKey(key: String): ParsingResult[PartyId] =
    PartyId.fromProtoPrimitive(key, "party_id")

  type PendingOnboardingClearanceStore =
    PendingOperationStore[OnboardingClearanceOperation, SynchronizerId]

  private def fromProtoV30(
      proto: v30.OnboardingClearanceOperation
  ): ParsingResult[OnboardingClearanceOperation] =
    for {
      onboardingEffectiveAt <- proto.onboardingEffectiveAt.traverse(
        EffectiveTime.fromProtoPrimitive
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))

    } yield OnboardingClearanceOperation(onboardingEffectiveAt)(rpv)

}
