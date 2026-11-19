// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters.AcsReplicationDestination
import com.digitalasset.canton.participant.store.ParticipantNodePersistentState
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.PartyId

import scala.concurrent.ExecutionContext

/** Creates the [[AcsReplicationContractHandler]] for the specified destination for an ACS
  * replication.
  *
  * Currently, only persistence destination is supported.
  */
trait AcsReplicationContractHandlerFactory {
  def create(
      acsReplicationDestination: AcsReplicationDestination,
      requestId: AddPartyRequestId,
      partyId: PartyId,
      replicationTimestamp: CantonTimestamp,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit executionContext: ExecutionContext): Either[String, AcsReplicationContractHandler]
}

final class DefaultAcsReplicationContractHandlerFactory(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    loggerFactory: NamedLoggerFactory,
) extends AcsReplicationContractHandlerFactory {

  override def create(
      acsReplicationDestination: AcsReplicationDestination,
      requestId: AddPartyRequestId,
      partyId: PartyId,
      replicationTimestamp: CantonTimestamp,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit executionContext: ExecutionContext): Either[String, AcsReplicationContractHandler] =
    acsReplicationDestination match {
      case AcsReplicationDestination.Persistence =>
        connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
          .toRight(s"Online party replication is not enabled on participant")
          .map { partyReplicationIndexingStore =>
            TargetParticipantAcsPersistence(
              requestId,
              partyId,
              replicationTimestamp,
              connectedSynchronizer,
              partyReplicationIndexingStore,
              participantNodePersistentState,
              loggerFactory,
            )
          }
      case AcsReplicationDestination.OutputStream(outputStream) =>
        Right(new TargetParticipantOutputStream(outputStream, loggerFactory))
    }
}
