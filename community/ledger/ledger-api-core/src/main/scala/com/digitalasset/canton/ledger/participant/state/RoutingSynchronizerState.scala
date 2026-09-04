// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.data.EitherT
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.error.TransactionRoutingError.{
  UnableToGetStaticParameters,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

/** Provides state information about a synchronizer. */
trait RoutingSynchronizerState {

  val topologySnapshots: Map[PhysicalSynchronizerId, TopologySnapshotLoader]

  /** @return
    *   true if there is at least a ready connected synchronizer, false otherwise
    */
  def existsReadySynchronizer(): Boolean

  def getPhysicalId(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId] =
    topologySnapshots.keys.filter(_.logical == synchronizerId).maxOption

  /** @return
    *   Right containing the topology snapshot for the given ``synchronizerId``, or Left with an
    *   error if the requested synchronizer is not connected
    */
  def getTopologySnapshotFor(
      psid: PhysicalSynchronizerId
  ): Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshotLoader]

  def getTopologySnapshotFor(
      targetPsid: Target[PhysicalSynchronizerId]
  ): Either[UnableToQueryTopologySnapshot.Failed, Target[TopologySnapshotLoader]] =
    getTopologySnapshotFor(targetPsid.unwrap).map(Target(_))

  def getSynchronizersOfContracts(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)]]

  /** Reads the stakeholders of the given contracts from the contract store.
    *
    * @return
    *   Left with the contracts unknown to the store, if any.
    */
  def getContractsStakeholders(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, Set[LfContractId], Map[LfContractId, Stakeholders]]

  /** SyncCryptoPureApi for this synchronizer.
    */
  def getSyncCryptoPureApi(
      synchronizerId: PhysicalSynchronizerId
  ): Either[UnableToGetStaticParameters.Failed, Option[SynchronizerCryptoPureApi]]
}
