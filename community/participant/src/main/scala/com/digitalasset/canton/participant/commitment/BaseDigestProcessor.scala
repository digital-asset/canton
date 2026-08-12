// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.functor.*
import cats.{Applicative, Functor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointToBeWritten
import com.digitalasset.canton.participant.commitment.DigestProcessorState.{
  Initial,
  Started,
  Starting,
  Stopped,
  Stopping,
}
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{Checkpoint, CheckpointType}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.KillSwitch

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait BaseDigestProcessor extends NamedLogging {

  implicit protected val executionContext: ExecutionContext

  protected def timeouts: ProcessingTimeout

  def synchronizerId: SynchronizerId

  def isReinitializingProcessor: Boolean

  protected def startPipelineInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(KillSwitch, Future[Unit])]

  @VisibleForTesting
  private[canton] def metrics: CommitmentMetrics
  protected def acsDigestStore: AcsDigestStore

  def writeCheckpoint(checkpointToBeWritten: CheckpointToBeWritten)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    acsDigestStore.insertCheckpointTime(checkpointToBeWritten.toCheckpoint).map { _ =>
      metrics.checkpointWatermark.updateValue(checkpointToBeWritten.recordTimeInclusive.toMicros)
    }

  private val state: AtomicReference[DigestProcessorState] = new AtomicReference(
    Initial
  )

  def isStartingOrStarted: Boolean = state.get() match {
    case _: Starting | _: Started => true
    case Initial | _: Stopping | _: Stopped => false
  }

  @VisibleForTesting
  private[commitment] def stateInternal: DigestProcessorState = state.get()

  /** @return
    *   a future that completes once the processing pipeline has started up. If the processor hasn't
    *   been started yet, or it has been
    *   [[com.digitalasset.canton.participant.commitment.DigestProcessorState.Stopped]], or is in
    *   the process of
    *   [[com.digitalasset.canton.participant.commitment.DigestProcessorState.Stopping]], the
    *   returned future is completed.
    */
  final def startingFuture: FutureUnlessShutdown[Unit] = state.get() match {
    case Initial => FutureUnlessShutdown.unit
    case Starting(startingComplete) => startingComplete.void
    case Started(_, _) => FutureUnlessShutdown.unit
    case Stopping(_) => FutureUnlessShutdown.unit
    case Stopped(_) => FutureUnlessShutdown.unit
  }

  /** @return
    *   a future that completes once the processing pipeline has completed. If the processor hasn't
    *   been started yet, the returned future is completed. If the processor has been stopped
    *   already, the returned future reflects the stop reason, i.e.
    *   [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown.unit]] for an ordinary shutdown, or
    *   a failed [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] if the processor was
    *   stopped with a failure.
    */
  final def completionFuture: FutureUnlessShutdown[Unit] =
    state.get() match {
      case Initial => FutureUnlessShutdown.unit
      case Starting(startingComplete) => startingComplete.flatMap(_.completionFuture)
      case Started(_, completionFuture) => completionFuture
      case Stopping(stoppingComplete) => stoppingComplete
      case Stopped(reason) => FutureUnlessShutdown.fromTry(reason)
    }

  final def start()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def runPipeline(
        starting: Starting,
        startingCompleted: PromiseUnlessShutdown[Started],
    ): FutureUnlessShutdown[Unit] =
      startPipelineInternal().thereafter {
        case Success(Outcome((ks, pipelineCompleted))) =>
          val completionPromise = PromiseUnlessShutdown.unsupervised[Unit]()
          val startedState = Started(ks, completionPromise.futureUS)

          if (state.compareAndSet(starting, startedState)) {
            // If the state was successfully set to `Started`,
            // try to update the state to `Stopped` upon completion of the pipeline.
            pipelineCompleted.onComplete { completionResult =>
              // The CAS is used to only update the state if the processor hasn't been explicitly stopped in the meantime.
              state.compareAndSet(startedState, Stopped(completionResult)).discard
            }
          }

          // `completionPromise` must be completed regardless of whether the state update was successful or not,
          // because `completionFuture` might have been called between setting the state to `Starting` and a call to `stop`.
          completionPromise.completeWithUS(FutureUnlessShutdown.outcomeF(pipelineCompleted)).discard

          // Similarly, `startingCompleted` must be completed with the `startedState` regardless of whether the state update was successful or not,
          // because `startingFuture` might have been called between setting the state to `Starting` and a call to `stop`.
          startingCompleted.outcome_(startedState)

        case Success(AbortedDueToShutdown) =>
          // if `startupPipelineInternal` was aborted due to shutdown,
          // try to set the processor's state to `Stopped`, since it hasn't started up successfully
          state.compareAndSet(starting, Stopped(TryUtil.unit)).discard
          startingCompleted.shutdown_()

        case Failure(ex) =>
          // startup of the processing pipeline failed, try to set the state to stopped
          state.compareAndSet(starting, Stopped(Failure(ex))).discard
          startingCompleted.failure(ex)
      }.void

    val startedPromise = PromiseUnlessShutdown.unsupervised[Started]()
    val starting = Starting(startedPromise.futureUS)
    if (state.compareAndSet(Initial, starting)) {
      runPipeline(starting, startedPromise)
    } else {
      logger.info("Digest processor has already been started before.")
      FutureUnlessShutdown.unit
    }
  }

  final def stop(): FutureUnlessShutdown[Unit] = {
    val stoppingCompletePromise = PromiseUnlessShutdown.unsupervised[Unit]()
    val successfulStop = Stopped(TryUtil.unit)
    val stoppingState = Stopping(stoppingCompletePromise.futureUS)

    val prevState = state.getAndUpdate {
      case Initial => successfulStop
      case Starting(_) | Started(_, _) => stoppingState
      case stoppingInProgress @ (Stopping(_) | Stopped(_)) => stoppingInProgress
    }

    prevState match {
      case Initial =>
        // nothing to do, the digest processor hasn't even been started yet, and it is considered stopped.
        FutureUnlessShutdown.unit
      case Starting(startingComplete) =>
        stoppingCompletePromise
          .completeWithUS(
            startingComplete
              .flatMap { case Started(ks, completionFuture) =>
                ks.shutdown()
                completionFuture
              }
              .thereafter {
                case Success(_) => state.set(successfulStop)
                case Failure(ex) => state.set(Stopped(Failure(ex)))
              }
          )
          .futureUS
      case Started(killSwitch, completionFuture) =>
        killSwitch.shutdown()
        stoppingCompletePromise
          .completeWithUS(
            completionFuture.thereafter {
              case Success(_) =>
                // a successful completion of the pipeline or AbortedDueToShutdown are both
                // considered safe states and lead to a successful `Stopped` state.
                state.set(successfulStop)
              case Failure(ex) => state.set(Stopped(Failure(ex)))
            }
          )
          .futureUS

      case Stopping(stoppingComplete) =>
        // nothing to do, the processor is already being stopped
        stoppingComplete
      case Stopped(reason) =>
        // nothing to do, the processor was already stopped before
        FutureUnlessShutdown.fromTry(reason)
    }
  }

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

  override def toString: String = s"${getClass.getSimpleName}($synchronizerId)"
}

sealed trait DigestProcessorState extends Product with Serializable
object DigestProcessorState {
  case object Initial extends DigestProcessorState
  final case class Starting(startingComplete: FutureUnlessShutdown[Started])
      extends DigestProcessorState
  final case class Started(killSwitch: KillSwitch, completionFuture: FutureUnlessShutdown[Unit])
      extends DigestProcessorState
  final case class Stopping(stoppingComplete: FutureUnlessShutdown[Unit])
      extends DigestProcessorState
  final case class Stopped(reason: Try[Unit]) extends DigestProcessorState
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

  /** When a checkpoint has been written, meaning that all digests up the offset (inclusive) have
    * been persisted.
    */
  final case class CheckpointWritten(
      recordTimeInclusive: CantonTimestamp,
      offsetInclusive: Offset,
      checkpointType: CheckpointType,
  )

  /** Used to signal that a checkpoint should be written, because all digests up to and including
    * `offsetInclusive` have been persisted.
    */
  final case class CheckpointToBeWritten(
      recordTimeInclusive: CantonTimestamp,
      offsetInclusive: Offset,
      checkpointType: CheckpointType,
  ) {
    def toCheckpointWritten: CheckpointWritten =
      CheckpointWritten(
        recordTimeInclusive,
        offsetInclusive,
        checkpointType,
      )

    def toCheckpoint: Checkpoint = Checkpoint(
      offset = offsetInclusive,
      recordTime = recordTimeInclusive,
      checkpointType = checkpointType,
    )
  }

  object CheckpointToBeWritten {
    def apply(
        timepoint: Timepoint,
        checkpointType: CheckpointType,
    ): CheckpointToBeWritten = CheckpointToBeWritten(
      timepoint.recordTime,
      timepoint.offset,
      checkpointType,
    )
  }

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
