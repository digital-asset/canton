// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.{Applicative, Functor}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.pekkoUtilSyntaxForFlowOpsFlow
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.ExecutionContext

trait BaseDigestProcessor extends NamedLogging {
  import BaseDigestProcessor.*

  implicit val executionContext: ExecutionContext

  // TODO(#33422) - decide later if we want to make this more general or not
  //  so we can use here eg. a simple TestDigestAccumulator
  protected def digestAccumulator: SequentialDigestAccumulator

  protected def inMemoryDigestAccumulator(implicit
      traceContext: TraceContext
  ): Flow[DigestAccumulator_Input, DigestAccumulator_Output, NotUsed] =
    Flow[DigestAccumulator_Input]
      .mapAsyncAndDrainUS(1)(digestAccumulator.process)
      .collect { case Some(checkpointWritten) => checkpointWritten }

  def start()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Returns a mapping between parties and the participants to which they are fully onboarded (i.e.
    * onboarding flag is false).
    * @param topologySnapshot
    *   the topology snapshot for looking up the party hosting information
    * @param parties
    *   the parties for which to load the hosting participants
    * @return
    */
  protected def getOnboardedParticipantsOfParties(
      topologySnapshot: TopologySnapshot,
      parties: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPartyId, Set[LedgerParticipantId]]] =
    topologySnapshot
      .activeParticipantsOfPartiesWithInfo(parties.toSeq)
      .map { partyIdToPartyInfoMap =>
        val onboardingCompleted = partyIdToPartyInfoMap.view
          .mapValues(info =>
            info.participants.view.collect {
              case (pid, attr) if !attr.onboarding => pid.toLf
            }.toSet
          )
          .toMap
        onboardingCompleted
      }
}

// TODO(#33422) - clean up and move here only the definitions an types that is used by all children
object BaseDigestProcessor {

  type Checkpointing_Input = ProcessingContext[InternalIndexService.AcsUpdate]
  type Checkpointing_Output = ProcessingContext[CheckpointFenceOr[InternalIndexService.AcsUpdate]]

  type Classifcation_Input = Checkpointing_Output
  type Classification_Output = ProcessingContext[CheckpointFenceOr[Classification]]

  type DigestAccumulator_Input = Classification_Output
  type DigestAccumulator_Output = CheckpointWritten

  /** Holds some data that we thread through the pipeline for a given input event.
    */
  final case class ProcessingContext[+T](
      timepoint: Timepoint,
      value: T,
  )(implicit val traceContext: TraceContext) {
    def offset: Offset = timepoint.offset
    def recordTime: CantonTimestamp = timepoint.recordTime

    def withValue[U](newValue: U): ProcessingContext[U] = copy(value = newValue)

    def map[U](f: T => U): ProcessingContext[U] = copy(value = f(value))

    def traverse[F[_], U](f: T => F[U])(implicit F: Functor[F]): F[ProcessingContext[U]] =
      F.map(f(value))(u => copy(value = u))
  }

  /** Data type to represent a checkpointing fence or some event to be processed. While this is
    * isomorphic to Either[Unit, A], the explicit type adds clarity.
    */
  sealed trait CheckpointFenceOr[+A] extends Product with Serializable {
    def map[B](f: A => B): CheckpointFenceOr[B] = this match {
      case fence: CheckpointFence => fence
      case NotCheckpointFence(topologySnapshot, value) =>
        NotCheckpointFence(topologySnapshot, f(value))
    }

    def getOption: Option[A] = this match {
      case _: CheckpointFence => None
      case NotCheckpointFence(_, x) => Some(x)
    }

    def traverse[F[_], B](f: A => F[B])(implicit F: Applicative[F]): F[CheckpointFenceOr[B]] =
      this match {
        case fence: CheckpointFence => F.pure(fence)
        case NotCheckpointFence(topologySnapshot, value) =>
          F.map(f(value))(b => NotCheckpointFence(topologySnapshot, b))
      }

    @VisibleForTesting
    private[commitment] def tryValue: A = this match {
      case _: CheckpointFence => throw new NoSuchElementException("CheckpointFence")
      case NotCheckpointFence(_, value) => value
    }

    @VisibleForTesting
    private[commitment] def toEither: Either[CheckpointType, A] = this match {
      case CheckpointFence(tpe) => Left(tpe)
      case NotCheckpointFence(_, value) => Right(value)
    }
  }
  final case class CheckpointFence(checkpointType: CheckpointType)
      extends CheckpointFenceOr[Nothing]

  final case class NotCheckpointFence[+A](topologySnapshot: TopologySnapshot, value: A)
      extends CheckpointFenceOr[A]
      with PrettyPrinting {
    def withValue[B](newValue: B): NotCheckpointFence[B] = copy(value = newValue)

    override protected def pretty: Pretty[NotCheckpointFence.this.type] =
      prettyOfClass(
        unnamedParam(c => prettyOfString[A](_.toString).treeOf(c.value))
      )
  }

  /** The output of classification describes which digests need to be updated
    */
  sealed trait Classification extends Product with Serializable

  /** Defines which digests (party and participant) need to be updated with the hash of the
    * contract.
    * @param stakeholders
    *   the parties and affected participants for which the digest needs to be updated with the hash
    *   of the contract and the locally hosted stakeholders.
    * @param locallyHostedStakeholders
    *   the stakeholders of the contract that are hosted by the processing participant. This
    *   collection does not contain duplicates.
    */
  final case class AcsUpdate(
      stakeholders: Map[LfPartyId, Set[LedgerParticipantId]],
      locallyHostedStakeholders: Seq[LfPartyId],
      cid: LfContractId,
      rc: ReassignmentCounter,
      isActivation: Boolean,
  ) extends Classification

  /** When a party is being onboarded to a participant.
    */
  final case class PartyOnboardingToParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  sealed trait PartyHostingChange extends Classification {
    def party: LfPartyId
    def participant: LedgerParticipantId
  }

  /** When a party has been added to a participant.
    */
  final case class PartyAddedToParticipant(
      override val party: LfPartyId,
      override val participant: LedgerParticipantId,
  ) extends PartyHostingChange

  /** When a party has been removed from a participant.
    */
  final case class PartyRemovedFromParticipant(
      override val party: LfPartyId,
      override val participant: LedgerParticipantId,
  ) extends PartyHostingChange

  /** When a checkpoint has been written, meaning that all digests up to record time and offset
    * (both inclusive) have been persisted.
    */
  final case class CheckpointWritten(
      recordTimeInclusive: CantonTimestamp,
      offsetInclusive: Offset,
      checkpointType: CheckpointType,
  )

  object CheckpointWritten {
    def apply(timepoint: Timepoint, tpe: CheckpointType): CheckpointWritten =
      CheckpointWritten(timepoint.recordTime, timepoint.offset, tpe)
  }

  /** Tracks changes to the hosting relationship per party.
    */
  class TopologyChangeTracker(
      private val deltas: Map[LfPartyId, Set[LedgerParticipantId] => Set[LedgerParticipantId]]
  ) {

    /** Returns a function that updates a party to participants map by adding the association from
      * the given party to the given participant.
      */
    def addPartyToParticipant(
        party: LfPartyId,
        participant: LedgerParticipantId,
    ): TopologyChangeTracker =
      new TopologyChangeTracker(
        deltas.updatedWith(party)(
          _.map(_.andThen(s => s + participant)).orElse(Some(s => s + participant))
        )
      )

    /** Returns a function that updates a party to participants map by removing the association from
      * the given party to the given participant.
      */
    def removePartyFromParticipant(
        party: LfPartyId,
        participant: LedgerParticipantId,
    ): TopologyChangeTracker = new TopologyChangeTracker(
      deltas.updatedWith(party)(
        _.map(_.andThen(s => s - participant)).orElse(Some(s => s - participant))
      )
    )

    def applyPendingTopologyChanges(
        map: Map[LfPartyId, Set[LedgerParticipantId]]
    ): Map[LfPartyId, Set[LedgerParticipantId]] =
      if (deltas.isEmpty) map
      else {
        map.view.map { case kv @ (party, participants) =>
          deltas.get(party).map(f => (party, f(participants))).getOrElse(kv)
        }.toMap
      }
  }

  object TopologyChangeTracker {
    val empty: TopologyChangeTracker = new TopologyChangeTracker(Map.empty)
  }
}
