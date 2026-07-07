// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.ledger.api.v2.state_service.{GetActiveContractsResponse, ParticipantPermission}
import com.daml.ledger.api.v2.topology_transaction.{TopologyEvent, TopologyTransaction}
import com.daml.ledger.api.v2.trace_context.TraceContext as LedgerApiTraceContext
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference

trait InternalIndexService {
  def activeContracts(
      partyIds: Set[LfPartyId],
      validAt: Option[Offset],
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed]

  def topologyTransactions(
      partyId: LfPartyId,
      fromExclusive: Offset,
  )(implicit traceContext: TraceContext): Source[TopologyTransaction, NotUsed]

  /** @return
    *   A tailing stream of ACS Update-s starting from fromExclusive and filtered to synchronizerId
    *   Synchronizer.
    */
  def acsUpdates(
      synchronizerId: SynchronizerId,
      fromExclusive: Option[Offset],
  )(implicit traceContext: TraceContext): Source[InternalIndexService.AcsUpdateContainer, NotUsed]

  /** @param stakeholders1
    *   must be nonempty
    * @param stakeholders2
    *   can be empty: in this case all active contracts will be returned which have at least one
    *   stakeholder present in stakeholders1
    * @return
    *   ActiveContracts of the ACS filtered to synchronizerId synchronizer, and which is the state
    *   of the ledger at activeAt Offset. Only those contracts will be returned, which have at least
    *   one stakeholder from each stakeholder set.
    */
  def acs(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      stakeholders1: Set[Party],
      stakeholders2: Set[Party],
  )(implicit traceContext: TraceContext): Source[InternalIndexService.ActiveContract, NotUsed]

  /** @return
    *   Unique parties emerging from stakeholders of Active Contracs in an ACS, defined by activeAt
    *   Offset and filtered to synchronizerId synchronizer. In case party is defined only contracts
    *   will be considered which have this party as part of their stakeholders.
    */
  def counterParties(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      party: Option[Party],
  )(implicit traceContext: TraceContext): Source[LfPartyId, NotUsed]
}

object InternalIndexService {
  final case class AcsUpdateContainer(
      acsUpdate: AcsUpdate,
      synchronizerTime: CantonTimestamp,
      offset: Offset,
      traceContext: TraceContext,
  )

  object AcsUpdateContainer {

    def fromUpdateResponse(updateResponse: UpdateResponse): Option[AcsUpdateContainer] =
      updateResponse match {
        case UpdateResponse.AcsChange(acsChange) =>
          Some(
            AcsUpdateContainer(
              acsUpdate = AcsUpdate.AcsChangeUpdate(acsChange.acsChange),
              synchronizerTime = acsChange.recordTime,
              offset = acsChange.offset,
              traceContext = acsChange.traceContext,
            )
          )
        case UpdateResponse.AcsCommitment(commitment) =>
          Some(
            AcsUpdateContainer(
              acsUpdate = AcsUpdate.AcsCommitment(commitment.payload),
              synchronizerTime = CantonTimestamp(commitment.recordTime),
              offset = commitment.offset,
              traceContext = commitment.traceContext,
            )
          )
        case UpdateResponse.ProtoUpdate(protoUpdate) =>
          protoUpdate.update match {
            case GetUpdatesResponse.Update.TopologyTransaction(topologyTransaction) =>
              Some(
                topologyAcsUpdateContainer(
                  acsUpdate = AcsUpdate.EffectivePartyToParticipantMappings(
                    mappings = effectivePartyToParticipantMappings(topologyTransaction)
                  ),
                  recordTime = topologyTransaction.recordTime,
                  offset = topologyTransaction.offset,
                  traceContext = topologyTransaction.traceContext,
                )
              )
            case _ => None
          }
      }

    private def topologyAcsUpdateContainer(
        acsUpdate: AcsUpdate,
        recordTime: Option[ProtoTimestamp],
        offset: Long,
        traceContext: Option[LedgerApiTraceContext],
    ): AcsUpdateContainer = {
      val synchronizerTime = (for {
        ts <- ProtoConverter.required("record_time", recordTime)
        cantonTimestamp <- CantonTimestamp.fromProtoTimestamp(ts)
      } yield cantonTimestamp).fold(
        err => throw new IllegalStateException(s"Could not parse update record time: $err"),
        identity,
      )
      AcsUpdateContainer(
        acsUpdate = acsUpdate,
        synchronizerTime = synchronizerTime,
        offset = Offset.tryFromLong(offset),
        traceContext = LedgerClient.traceContextFromLedgerApi(traceContext),
      )
    }

    private def authorizationLevelOf(permission: ParticipantPermission): AuthorizationLevel =
      permission match {
        case ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION => Submission
        case ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION => Confirmation
        case ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION => Observation
        case other =>
          throw new IllegalStateException(
            s"Unexpected participant permission in topology event: $other"
          )
      }

    private def partyToParticipantAuthorization(
        partyId: String,
        participantId: String,
        authorizationEvent: AuthorizationEvent,
    ): PartyToParticipantAuthorization =
      PartyToParticipantAuthorization(
        party = Ref.Party.assertFromString(partyId),
        participant = Ref.ParticipantId.assertFromString(participantId),
        authorizationEvent = authorizationEvent,
      )

    private def effectivePartyToParticipantMappings(
        topologyTransaction: TopologyTransaction
    ): Set[PartyToParticipantAuthorization] =
      topologyTransaction.events.view
        .map(_.event)
        .flatMap {
          case TopologyEvent.Event.Empty => None
          case TopologyEvent.Event.ParticipantAuthorizationAdded(added) =>
            Some(
              partyToParticipantAuthorization(
                partyId = added.partyId,
                participantId = added.participantId,
                authorizationEvent = Added(authorizationLevelOf(added.participantPermission)),
              )
            )
          case TopologyEvent.Event.ParticipantAuthorizationChanged(changed) =>
            Some(
              partyToParticipantAuthorization(
                partyId = changed.partyId,
                participantId = changed.participantId,
                authorizationEvent = ChangedTo(authorizationLevelOf(changed.participantPermission)),
              )
            )
          case TopologyEvent.Event.ParticipantAuthorizationOnboarding(onboarding) =>
            Some(
              partyToParticipantAuthorization(
                partyId = onboarding.partyId,
                participantId = onboarding.participantId,
                authorizationEvent =
                  Onboarding(authorizationLevelOf(onboarding.participantPermission)),
              )
            )
          case TopologyEvent.Event.ParticipantAuthorizationRevoked(revoked) =>
            Some(
              partyToParticipantAuthorization(
                partyId = revoked.partyId,
                participantId = revoked.participantId,
                authorizationEvent = Revoked,
              )
            )
        }
        .toSet
  }

  sealed trait AcsUpdate extends Product with Serializable
  object AcsUpdate {
    final case class AcsCommitment(payload: ByteString) extends AcsUpdate
    final case class EffectivePartyToParticipantMappings(
        mappings: Set[
          Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
        ]
    ) extends AcsUpdate
    final case class AcsChangeUpdate(
        acsChange: AcsChange
    ) extends AcsUpdate
  }

  final case class ActiveContract(
      contractId: ContractId,
      stakeholders: Set[Party],
      reassignmentCounter: ReassignmentCounter,
  )
}

trait InternalIndexServiceProvider {
  def internalIndexService: Option[InternalIndexService]
  def registerInternalIndexService(internalIndexService: InternalIndexService): Unit
  def unregisterInternalIndexService(): Unit
}

trait InternalIndexServiceProviderImpl extends InternalIndexServiceProvider {
  private val internalIndexServiceRef: AtomicReference[Option[InternalIndexService]] =
    new AtomicReference(None)

  override def internalIndexService: Option[InternalIndexService] =
    internalIndexServiceRef.get()

  override def registerInternalIndexService(internalIndexService: InternalIndexService): Unit =
    internalIndexServiceRef.set(Some(internalIndexService))

  override def unregisterInternalIndexService(): Unit =
    internalIndexServiceRef.set(None)
}
