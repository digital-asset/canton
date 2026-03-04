// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

/** Participant nodes attempt to perform a handshake with successor synchronizer. The goal is to
  * detect inconsistent static synchronizer parameters or incompatible protocol versions. This
  * 'pending handshake' is persisted so that it can be attempted again in the case of a
  * crash/restart. It will be attempted over and over again until it succeeds (or LSU happens/is
  * cancelled).
  */
final case class PendingHandshakeWithLsuSuccessor(successorPSId: PhysicalSynchronizerId)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PendingHandshakeWithLsuSuccessor.type
    ]
) extends HasProtocolVersionedWrapper[PendingHandshakeWithLsuSuccessor] {
  @transient override protected lazy val companionObj: PendingHandshakeWithLsuSuccessor.type =
    PendingHandshakeWithLsuSuccessor

  private def toProtoV30: v30.PendingHandshakeWithLsuSuccessor =
    v30.PendingHandshakeWithLsuSuccessor(successorPhysicalSynchronizerId =
      successorPSId.toProtoPrimitive
    )

  def toPendingOperation(
      currentPSId: PhysicalSynchronizerId
  ): PendingOperation[PendingHandshakeWithLsuSuccessor, PhysicalSynchronizerId] =
    PendingOperation(
      name = PendingHandshakeWithLsuSuccessor.operationName,
      key = PendingHandshakeWithLsuSuccessor.operationKey,
      operation = this,
      synchronizer = currentPSId,
    )
}

object PendingHandshakeWithLsuSuccessor
    extends VersioningCompanion[PendingHandshakeWithLsuSuccessor] {
  override val name: String = "PendingHandshakeWithLsuSuccessor"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.PendingHandshakeWithLsuSuccessor)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  lazy val operationName: NonEmptyString =
    NonEmptyString.tryCreate("pending-handshake-with-lsu-successor")
  lazy val operationKey: String = ""

  type PendingHandshakesWithSuccessorsStore =
    PendingOperationStore[PendingHandshakeWithLsuSuccessor, PhysicalSynchronizerId]

  private def fromProtoV30(
      proto: v30.PendingHandshakeWithLsuSuccessor
  ): ParsingResult[PendingHandshakeWithLsuSuccessor] =
    for {
      successorPSId <- PhysicalSynchronizerId.fromProtoPrimitive(
        proto.successorPhysicalSynchronizerId,
        "successor_physical_synchronizer_id",
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))

    } yield PendingHandshakeWithLsuSuccessor(successorPSId)(rpv)

}
