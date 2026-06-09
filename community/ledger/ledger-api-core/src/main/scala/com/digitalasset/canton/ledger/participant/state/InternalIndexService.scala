// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
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
      fromExclusive: Offset,
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
  ): Source[LfPartyId, NotUsed]
}

object InternalIndexService {
  final case class AcsUpdateContainer(
      acsUpdate: AcsUpdate,
      synchronizerTime: CantonTimestamp,
      offset: Offset,
      traceContext: TraceContext,
  )

  sealed trait AcsUpdate
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
