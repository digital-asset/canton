// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.error.{TransactionError, TransactionRoutingError}
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state.SyncService.{
  ConnectedSynchronizerRequest,
  ConnectedSynchronizerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfContractId, LfSubmittedTransaction}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPartyId, SynchronizerAlias}

/** An interface to change a ledger via a participant. '''Please note that this interface is
  * unstable and may significantly change.'''
  *
  * The methods in this interface are all methods that are supported *uniformly* across all ledger
  * participant implementations. Methods for uploading packages, on-boarding parties, and changing
  * ledger-wide configuration are specific to a ledger and therefore to a participant
  * implementation. Moreover, these methods usually require admin-level privileges, whose granting
  * is also specific to a ledger.
  *
  * If a ledger is run for testing only, there is the option for quite freely allowing the
  * on-boarding of parties and uploading of packages. There are plans to make this functionality
  * uniformly available: see the roadmap for progress information
  * https://github.com/digital-asset/daml/issues/121.
  *
  * The following methods are currently available for changing the state of a Daml ledger:
  *   - submitting a transaction using [[SyncService!.submitTransaction]]
  *   - allocating a new party using [[PartySyncService!.allocateParty]]
  *   - pruning a participant ledger using [[ParticipantPruningSyncService!.prune]]
  */
trait SyncService
    extends SubmissionSyncService
    with PackageSyncService
    with PartySyncService
    with ParticipantPruningSyncService
    with ReportsHealth
    with InternalStateServiceProvider {

  // temporary implementation, will be removed as topology events on Ledger API proceed
  def getConnectedSynchronizers(request: ConnectedSynchronizerRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ConnectedSynchronizerResponse] =
    throw new UnsupportedOperationException()

  // temporary implementation, will be removed as topology events on Ledger API proceed
  /** Get the offsets of the incomplete assigned/unassigned events for a set of stakeholders.
    *
    * @param validAt
    *   The offset of validity in participant offset terms.
    * @param stakeholders
    *   Only offsets are returned which have at least one stakeholder from this set.
    * @return
    *   All the offset of assigned/unassigned events which do not have their counterparts visible at
    *   the validAt offset, and only for the reassignments for which this participant is
    *   reassigning.
    */
  def incompleteReassignmentOffsets(
      validAt: Offset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Vector[Offset]] = {
    val _ = validAt
    val _ = stakeholders
    val _ = traceContext
    FutureUnlessShutdown.pure(Vector.empty)
  }

  // temporary implementation, will be removed with the refactoring of the SyncService interface
  /** Computes a SynchronizerId -> PartyId -> PackageId relation that describes:
    *   - for each synchronizer that hosts all the provided `submitters` that can submit. The
    *     provided `submitters` can be empty (for externally signed transactions), in which case
    *     synchronizers are not restricted by parties with submission rights on the local
    *     participant
    *   - which package-ids can be accepted (i.e. they are vetting-valid) in a transaction by each
    *     of the informees provided
    *   - if the prescribed synchronizer is provided, only that one is considered
    */
  def packageMapFor(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizer: Option[SynchronizerId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[LfPackageId]]]]

  // temporary implementation, will be removed with the refactoring of the SyncService interface
  /** Computes the highest ranked synchronizer from the given admissible synchronizers without
    * performing topology checks.
    *
    * This method is used internally in command processing to pre-select a synchronizer for
    * determining the package preference set used in command interpretation.
    *
    * For the definitive synchronizer selection to be used for routing of a submitted transaction,
    * use [[selectRoutingSynchronizer]].
    *
    * @param submitterInfo
    *   The submitter info
    * @param transaction
    *   The submitted transaction
    * @param transactionMeta
    *   The transaction metadata
    * @param admissibleSynchronizers
    *   The list of synchronizers from which the best one should be selected
    * @param disclosedContractIds
    *   The list of disclosed contracts used in command interpretation
    * @param routingSynchronizerState
    *   The routing synchronizer state the computation should be based on
    * @return
    *   The ID of the best ranked synchronizer
    */
  def computeHighestRankedSynchronizerFromAdmissible(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]],
      disclosedContractIds: List[LfContractId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, PhysicalSynchronizerId]

  // temporary implementation, will be removed with the refactoring of the SyncService interface
  /** Computes the best synchronizer for a submitted transaction by checking the submitted
    * transaction against the topology of the connected synchronizers and ranking the admissible
    * ones using the synchronizer ranking (by priority, minimum number of reassignments and
    * synchronizer-id).
    *
    * @param submitterInfo
    *   The submitter info
    * @param transaction
    *   The submitted transaction
    * @param transactionMeta
    *   The transaction metadata
    * @param disclosedContractIds
    *   The list of disclosed contracts used in command interpretation
    * @param optSynchronizerId
    *   If provided, only this synchronizer id is considered as a candidate for routing
    * @param transactionUsedForExternalSigning
    *   If true, the topology checks do not required that the submitters of the transaction have
    *   submission rights on the local participant since they are supposed to externally sign the
    *   transaction.
    * @return
    *   The rank of the routing synchronizer
    */
  def selectRoutingSynchronizer(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      disclosedContractIds: List[LfContractId],
      optSynchronizerId: Option[SynchronizerId],
      transactionUsedForExternalSigning: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionError, SynchronizerRank]

  // temporary implementation, will be removed with the refactoring of the SyncService interface
  /** Constructs and fetches the current synchronizer state, to be used throughout command execution
    */
  def getRoutingSynchronizerState(implicit traceContext: TraceContext): RoutingSynchronizerState
}

object SyncService {
  final case class ConnectedSynchronizerRequest(
      party: LfPartyId,
      participantId: Option[ParticipantId],
  )

  final case class ConnectedSynchronizerResponse(
      connectedSynchronizers: Seq[ConnectedSynchronizerResponse.ConnectedSynchronizer]
  )

  object ConnectedSynchronizerResponse {
    final case class ConnectedSynchronizer(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: PhysicalSynchronizerId,
        permission: ParticipantPermission,
    )
  }
}
