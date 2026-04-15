// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

/** Represents an LSU operation that a participant node must perform against a successor
  * synchronizer (e.g. handshake with the successor, topology copy from the predecessor). This
  * 'pending operation' is persisted so that it can be attempted again in the case of a
  * crash/restart. It will be attempted over and over again until it succeeds (or LSU happens/is
  * cancelled).
  */
final case class PendingLsuOperation(successorPsid: PhysicalSynchronizerId)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PendingLsuOperation.type
    ]
) extends HasProtocolVersionedWrapper[PendingLsuOperation] {
  @transient override protected lazy val companionObj: PendingLsuOperation.type =
    PendingLsuOperation

  private def toProtoV30: v30.PendingLsuOperation =
    v30.PendingLsuOperation(successorPhysicalSynchronizerId = successorPsid.toProtoPrimitive)

  def toPendingOperation(
      currentPsid: PhysicalSynchronizerId
  ): PendingOperation[PendingLsuOperation, PhysicalSynchronizerId] =
    PendingOperation(
      name = PendingLsuOperation.operationName,
      key = PendingLsuOperation.operationKey,
      operation = this,
      synchronizer = currentPsid,
    )
}

object PendingLsuOperation extends VersioningCompanion[PendingLsuOperation] {
  override val name: String = "PendingLsuOperation"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.PendingLsuOperation)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  lazy val operationName: NonEmptyString =
    NonEmptyString.tryCreate("pending-lsu-operation")
  lazy val operationKey: String = ""

  type Store = PendingOperationStore[PendingLsuOperation, PhysicalSynchronizerId]

  private def fromProtoV30(
      proto: v30.PendingLsuOperation
  ): ParsingResult[PendingLsuOperation] =
    for {
      successorPsid <- PhysicalSynchronizerId.fromProtoPrimitive(
        proto.successorPhysicalSynchronizerId,
        "successor_physical_synchronizer_id",
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield PendingLsuOperation(successorPsid)(rpv)
}
