// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** The domain topology client that reads data from a topology store
  *
  * @param domainId The domain-id corresponding to this store
  * @param store The store
  */
class StoreBasedDomainTopologyClientX(
    val clock: Clock,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends BaseDomainTopologyClient
    with NamedLogging {

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshotX = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, topology known until=$topologyKnownUntilTimestamp",
    )
    new StoreBasedTopologySnapshotX(
      timestamp,
      store,
      packageDependencies,
      loggerFactory,
    )
  }

}

/** Topology snapshot loader
  *
  * @param timestamp the asOf timestamp to use
  * @param store the db store to use
  * @param packageDependencies lookup function to determine the direct and indirect package dependencies
  */
class StoreBasedTopologySnapshotX(
    val timestamp: CantonTimestamp,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging
    with NoTracing {

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] = ???

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] = ???

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] = ???

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ) = ???

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ) = ???

  /** returns the list of currently known mediators */
  override def mediators(): Future[Seq[MediatorId]] = ???

  /** Returns a list of all known parties on this domain */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]] = ???

  /** Returns a list of all known parties on this domain */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] = ???

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] = ???

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] = ???

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] = ???

  override def findParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[LegalIdentityClaimEvidence.X509Cert]] = ???

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: KeyOwner): Future[KeyCollection] = ???

}
