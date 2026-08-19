// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.applicativeError.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

import TransactionRoutingError.{TopologyErrors, UnableToQueryTopologySnapshot}

private final case class SynchronizerSuitability(
    unknownSubmitters: Set[LfPartyId],
    submittersWithoutSubmissionPermission: Seq[LfPartyId],
    decentralizedPartiesWithSubmittingParticipant: Seq[LfPartyId],
) {
  def isSuitable: Boolean =
    unknownSubmitters.isEmpty &&
      submittersWithoutSubmissionPermission.isEmpty &&
      decentralizedPartiesWithSubmittingParticipant.isEmpty

  def isUnsuitableOnlyDueToDecentralizedParties: Boolean =
    unknownSubmitters.isEmpty &&
      submittersWithoutSubmissionPermission.isEmpty &&
      decentralizedPartiesWithSubmittingParticipant.nonEmpty
}

class AdmissibleSynchronizersComputation(
    localParticipantId: ParticipantId,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Returns the hosting participants for the requested informees and submitters on the admissible
    * synchronizers, where an admissible synchronizer satisfies the following:
    *   - submitters have to be hosted on the local participant
    *   - informees have to be hosted on some participant. It is assumed that the participant is
    *     connected to all synchronizers in `connectedSynchronizers`
    */
  def forParties(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Map[
    PhysicalSynchronizerId,
    Map[LfPartyId, Set[ParticipantId]],
  ]]] = {

    def queryPartyTopologySnapshotClient(
        synchronizerPartyTopologySnapshotClient: (
            PhysicalSynchronizerId,
            PartyTopologySnapshotClient,
        )
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Option[
      (PhysicalSynchronizerId, Map[LfPartyId, PartyInfo])
    ]] = {
      val (synchronizerId, partyTopologySnapshotClient) = synchronizerPartyTopologySnapshotClient
      val allParties = submitters.view ++ informees.view
      partyTopologySnapshotClient
        .activeParticipantsOfPartiesWithInfo(allParties.toSeq)
        .attemptT
        .map { partyTopology =>
          val partyTopologyWithThresholds = partyTopology
            .filter { case (_, partyInfo) => partyInfo.participants.nonEmpty }

          Option.when(partyTopologyWithThresholds.nonEmpty) {
            synchronizerId -> partyTopologyWithThresholds
          }
        }
        .leftMap { throwable =>
          logger.warn("Unable to query the topology information", throwable)
          UnableToQueryTopologySnapshot.Failed(synchronizerId)
        }
    }

    def queryTopology(): EitherT[FutureUnlessShutdown, TransactionRoutingError, Map[
      PhysicalSynchronizerId,
      Map[LfPartyId, PartyInfo],
    ]] =
      // TODO(#33650) - replace with unboundedTraverseFilter, safe because the number of topology snapshots is bounded by the participant's connected synchronizers
      synchronizerState.topologySnapshots.toVector
        .parTraverseFilter(queryPartyTopologySnapshotClient)
        .map(_.toMap)

    def ensureAllKnown[A, E](
        required: Set[A],
        known: Set[A],
        ifUnknown: Set[A] => E,
    ): EitherT[FutureUnlessShutdown, E, Unit] = {
      val unknown = required -- known
      EitherT.cond[FutureUnlessShutdown](
        unknown.isEmpty,
        (),
        ifUnknown(unknown),
      )
    }

    def ensureAllSubmittersAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = submitters,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownSubmitters.Error.apply,
      )

    def ensureAllInformeesAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = informees,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownInformees.Error.apply,
      )

    def ensureNonEmpty[I[_] <: collection.immutable.Iterable[?], A, E](
        iterable: I[A],
        ifEmpty: => E,
    ): EitherT[FutureUnlessShutdown, E, NonEmpty[I[A]]] =
      EitherT.fromEither[FutureUnlessShutdown](NonEmpty.from(iterable).toRight(ifEmpty))

    def synchronizerWithAll(parties: Set[LfPartyId])(
        topology: (PhysicalSynchronizerId, Map[LfPartyId, PartyInfo])
    ): Boolean =
      parties.subsetOf(topology._2.keySet)

    def synchronizersWithAll(
        parties: Set[LfPartyId],
        topology: Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]],
        ifEmpty: Set[PhysicalSynchronizerId] => TransactionRoutingError,
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] = {
      val synchronizersWithAllParties = topology.filter(synchronizerWithAll(parties))
      ensureNonEmpty(synchronizersWithAllParties, ifEmpty(topology.keySet))
    }

    def synchronizersWithAllSubmitters(
        topology: Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] =
      synchronizersWithAll(
        parties = submitters,
        topology = topology,
        ifEmpty = TopologyErrors.SubmittersNotActive.Error(_, submitters),
      )

    def synchronizersWithAllInformees(
        topology: Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] =
      synchronizersWithAll(
        parties = informees,
        topology = topology,
        ifEmpty = TopologyErrors.InformeesNotActive.Error(_, informees),
      )

    def suitableSynchronizers(
        synchronizersWithAllSubmitters: NonEmpty[
          Map[PhysicalSynchronizerId, Map[LfPartyId, PartyInfo]]
        ]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Set[PhysicalSynchronizerId]
    ]] = {
      logger.debug(
        s"Checking whether one synchronizer in ${synchronizersWithAllSubmitters.keys} is suitable for submission"
      )

      // Return all reasons why a synchronizer cannot be used, empty reasons means suitable.
      def unsuitableSynchronizerReason(
          synchronizerId: PhysicalSynchronizerId,
          parties: Map[LfPartyId, PartyInfo],
      ): SynchronizerSuitability = {
        // We keep only the relevant topology (submitter on the local participant)
        val locallyHostedSubmitters: Map[LfPartyId, (ParticipantAttributes, PartyInfo)] =
          parties.toSeq.mapFilter { case (party, partyInfo) =>
            for {
              permissions <- partyInfo.participants.get(localParticipantId)
              _ <- Option.when(submitters.contains(party))(())
            } yield (party, (permissions, partyInfo))
          }.toMap

        val unknownSubmitters: Set[LfPartyId] = submitters.diff(locallyHostedSubmitters.keySet)

        /* A party that is hosted with Submission permission on some participant has no signing key of
         * its own: its submission authorization is created by the submitting participant. Such a party
         * therefore cannot have a confirmation threshold greater than 1, because a single participant
         * cannot produce authorization on behalf of several independent participants.
         * If no hosting participant has Submission permission, the party can only act through external
         * signing, so this diagnosis does not apply and the generic error is reported instead.
         */
        def hasSubmittingParticipant(partyInfo: PartyInfo): Boolean =
          partyInfo.participants.values.exists(_.permission >= Submission)

        val (decentralizedPartiesWithSubmittingParticipant, submittersWithoutSubmissionPermission) =
          locallyHostedSubmitters.toSeq.foldLeft(
            (Seq.empty[LfPartyId], Seq.empty[LfPartyId])
          ) { case ((decentralizedParties, withoutPermission), (party, (permissions, partyInfo))) =>
            if (partyInfo.threshold > PositiveInt.one && hasSubmittingParticipant(partyInfo))
              (decentralizedParties :+ party, withoutPermission)
            else if (permissions.permission < Submission)
              (decentralizedParties, withoutPermission :+ party)
            else (decentralizedParties, withoutPermission)
          }

        val reason = SynchronizerSuitability(
          unknownSubmitters = unknownSubmitters,
          submittersWithoutSubmissionPermission = submittersWithoutSubmissionPermission,
          decentralizedPartiesWithSubmittingParticipant =
            decentralizedPartiesWithSubmittingParticipant,
        )

        if (!reason.isSuitable) {
          val context: Map[String, Any] = Map(
            "unknown submitters" -> unknownSubmitters,
            "without submission permission" -> submittersWithoutSubmissionPermission,
            "submission permission and confirmation threshold > 1" ->
              decentralizedPartiesWithSubmittingParticipant,
          )
          logger.debug(s"Cannot use synchronizer $synchronizerId: $context")
        }

        reason
      }

      val (unsuitableReasons, suitableSynchronizerIds) =
        synchronizersWithAllSubmitters.toSeq.partitionMap { case (synchronizerId, topology) =>
          val reason = unsuitableSynchronizerReason(synchronizerId, topology)
          Either.cond(reason.isSuitable, synchronizerId, reason)
        }

      // Only evaluated if no synchronizer is suitable
      def noSuitableSynchronizerError: TransactionRoutingError =
        if (unsuitableReasons.forall(_.isUnsuitableOnlyDueToDecentralizedParties))
          TopologyErrors.DecentralizedPartyCannotSubmit.Error(
            unsuitableReasons.flatMap(_.decentralizedPartiesWithSubmittingParticipant).distinct
          )
        else noSynchronizerWhereAllSubmittersCanSubmit

      ensureNonEmpty(suitableSynchronizerIds.toSet, noSuitableSynchronizerError)
    }

    def commonSynchronizerIds(
        submittersSynchronizerIds: Set[PhysicalSynchronizerId],
        informeesSynchronizerIds: Set[PhysicalSynchronizerId],
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Set[PhysicalSynchronizerId]
    ]] =
      ensureNonEmpty(
        submittersSynchronizerIds.intersect(informeesSynchronizerIds),
        TopologyErrors.NoCommonSynchronizer.Error(submitters, informees),
      )

    def noSynchronizerWhereAllSubmittersCanSubmit: TransactionRoutingError =
      submitters.toSeq match {
        case Seq(one) => TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit.NotAllowed(one)
        case some =>
          TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit.NoSuitableSynchronizer(some)
      }

    for {
      topology <- queryTopology()
      _ = logger.debug(s"Topology queried for the following synchronizers: ${topology.keySet}")
      knownParties = topology.view.values.map(_.keySet).fold(Set.empty)(_ ++ _)
      _ <- ensureAllSubmittersAreKnown(knownParties)
      _ <- ensureAllInformeesAreKnown(knownParties)

      synchronizersWithAllSubmitters <- synchronizersWithAllSubmitters(topology)
      _ = logger.debug(
        s"Synchronizers with all submitters: ${synchronizersWithAllSubmitters.keySet}"
      )

      synchronizersWithAllInformees <- synchronizersWithAllInformees(topology)
      _ = logger.debug(s"Synchronizers with all informees: ${synchronizersWithAllInformees.keySet}")

      submittersSynchronizerIds <- suitableSynchronizers(synchronizersWithAllSubmitters)
      informeesSynchronizerIds = synchronizersWithAllInformees.keySet
      commonSynchronizerIds <- commonSynchronizerIds(
        submittersSynchronizerIds,
        informeesSynchronizerIds,
      )
    } yield NonEmpty
      .from(
        topology.view
          .filterKeys(commonSynchronizerIds)
          .mapValues(_.view.mapValues(_.participants.keySet).toMap)
          .toMap
      )
      .getOrElse(sys.error("Unexpected empty result"))
  }
}
