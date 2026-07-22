// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  CheckpointFenceOr,
  CheckpointWritten,
  Classification,
  DigestAccumulator_Input,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyHostingChange,
  PartyOnboardingToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.AcsDigestTracingMode
import com.digitalasset.canton.participant.digest.{DigestDelta, DigestOperation, DigestOps}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  InternedParticipantId,
  LocalPartyFirst,
  PartyAndOrder,
  PartyOrder,
  RawDigest,
  RemotePartyFirst,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.Digest
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.{InternedPartyId, LedgerParticipantId}
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Flow

import java.util.concurrent.atomic.AtomicReference
import scala.Ordered.orderingToOrdered
import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/** @param acsUpdateBatchSize
  *   How many updates to digests (not necessarily distinct digests though) can be conflated before
  *   they must be persisted to the database.
  * @param digestLoadParallelism
  *   How many queries shall be made in parallel to load digests. Must be a power of two.
  */
class InMemoryDigestAccumulator(
    thisParticipant: LedgerParticipantId,
    digestStore: AcsDigestStore,
    override protected val loggerFactory: NamedLoggerFactory,
    stringInterningEval: Eval[StringInterning],
    acsUpdateBatchSize: Int,
    digestLoadParallelism: Int,
    digestStoreParallelism: Int,
    tracingMode: AcsDigestTracingMode,
    enableConsistencyChecks: Boolean,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import InMemoryDigestAccumulator.*

  private val directExecutionContext: ExecutionContext = DirectExecutionContext(noTracingLogger)

  private[this] val digests
      : scala.collection.concurrent.Map[DigestIdentifier, CountingAccumulatingDigest] =
    TrieMap.empty

  def flow()(implicit
      traceContext: TraceContext
  ): Flow[DigestAccumulator_Input, CheckpointWritten, NotUsed] = flowInternal()

  @VisibleForTesting
  def flowInternal(
      computeDigestBlocker: Option[
        ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit]
      ] = None,
      sequentialBlocker: Option[
        ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit]
      ] = None,
      storeBlocker: Option[ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit]] =
        None,
  )(implicit
      traceContext: TraceContext
  ): Flow[DigestAccumulator_Input, CheckpointWritten, NotUsed] =
    Flow[DigestAccumulator_Input]
      .map(ensurePresent)
      .withBlocker(computeDigestBlocker)(directExecutionContext)
      // Set buffer size to the digest load parallelisms so that at most `digestLoadParallelism` many
      // reads are spawned by `ensurePresent` when the buffer is filling up. This bound is conservative
      // in that it also counts `ensurePresent` that do not have to load anything at all.
      .buffer(digestLoadParallelism, OverflowStrategy.backpressure)
      .map(computeDigestChanges)
      .mapAsyncAndDrainUS(
        // Parallelism one here ensures that the above buffer size limit actually translates to the number of parallel reads.
        // Otherwise the parallelism could add up.
        parallelism = 1
      )(joinLoading)
      .async
      .withBlocker(sequentialBlocker)(directExecutionContext)
      // The accumulator is a non-empty Seq of at most two elements. The first one accumulates the changes so far
      // and the second element is only used if it cannot be accumulated into the first one.
      .aggregate(makeUpdatedAccumulators)(
        full = fullAccumulator,
        aggregate = aggregateUpdatedAccumulators,
        emit = _.map(copySnapshot),
      )
      .mapConcat(_.forgetNE)
      .withBlocker(storeBlocker)(directExecutionContext)
      .async
      .mapAsyncAndDrainUS(parallelism = digestStoreParallelism)(persistDigestUpdates)
      .mapAsyncAndDrainUS(
        // Checkpoints must be written sequentially!
        parallelism = 1
      )(persistCheckpoint)
      .mapConcat(deregister)

  /** Finds all the
    * [[com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator.DigestIdentifier]]s
    * necessary for processing the given `input` and initiates the loading into [[digests]] if
    * necessary. If no loading is needed, registers a usage with the existing accumulators to
    * prevent early eviction.
    *
    * The returned [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] completes when all the
    * necessary accumulators are ready for use.
    */
  private def ensurePresent(input: DigestAccumulator_Input): EnsurePresentOutput =
    input.map(_.map { classification =>
      implicit val traceContext: TraceContext = input.traceContext
      val requiredIdentifiersO = requiredIdentifiers(classification)
      val output = requiredIdentifiersO.map(registerAndLoadIdentifiers(_, input.timepoint))
      classification -> output
    })

  /** Computes the
    * [[com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator.DigestIdentifier]]s
    * required for processing the given
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor.Classification]].
    */
  private def requiredIdentifiers(
      classification: Classification
  ): Option[immutable.Iterable[DigestIdentifier]] = {
    val interning = stringInterningEval.value
    classification match {
      case acsUpdate: AcsUpdate =>
        val stakeholdersToParticipants = acsUpdate.stakeholders
        val participants = stakeholdersToParticipants
          .flatMap { case (_, participants) => participants }
          .toSet[LedgerParticipantId]
          .map { participant =>
            val internedParticipantId = interning.participantId.internalize(participant)
            ParticipantDigestIdentifier(internedParticipantId)
          }
        val stakeholders = stakeholdersToParticipants.flatMap { case (stakeholder, _) =>
          val internedStakeholder = interning.party.internalize(stakeholder)
          Seq(LocalPartyFirst, RemotePartyFirst).map(
            PartyDigestIdentifier(internedStakeholder, _)
          )
        }
        Some(stakeholders ++ participants)
      case _: PartyOnboardingToParticipant =>
        None
      case addedRemoved: PartyHostingChange =>
        val remoteParticipant = addedRemoved.participant
        val participant = ParticipantDigestIdentifier(
          interning.participantId.internalize(remoteParticipant)
        )
        val internedParty = interning.party.internalize(addedRemoved.party)
        val partyOrder =
          if (thisParticipant < remoteParticipant) LocalPartyFirst
          else RemotePartyFirst
        val party = PartyDigestIdentifier(internedParty, partyOrder)
        val identifiers = Seq(participant, party)
        Some(identifiers)
    }
  }

  private def registerAndLoadIdentifiers(
      identifiers: immutable.Iterable[DigestIdentifier],
      timepoint: Timepoint,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[AccumulatingDigests] = {
    // Check that identifiers are distinct; otherwise usage counting will be broken.
    if (enableConsistencyChecks) {
      requireDistinct(identifiers)(identifier => s"Duplicate identifier: $identifier")
    }

    val (alreadyInMap, toBeLoaded) = identifiers.toSeq.partitionMap { identifier =>
      val currentO = digests.updateWith(identifier) {
        case None => Some(CountingAccumulatingDigest.initial(timepoint))
        case Some(cad) => Some(cad.incrementUsage(prettyIdentifier(identifier), PositiveInt.one))
      }
      val current = currentO.getOrElse(
        throw new IllegalStateException("Unexpected missing value after insertion")
      )
      // Identify whether we have added the accumulator to the digests (and so we are responsible for initializing it)
      // or whether it has already been there.
      val accumulator = current.accumulator
      Either.cond(
        current.usageCounter == 1 && !accumulator.isInitialized,
        identifier -> accumulator,
        identifier -> accumulator,
      )
    }
    val (participantsToLoad, partiesToLoad) = toBeLoaded.partitionMap { case (identifier, _) =>
      identifier.toEither
    }

    val partiesF = digestStore.party.bulkLookup(partiesToLoad, timepoint.offset)
    val participantsF = digestStore.participant.bulkLookup(participantsToLoad, timepoint.offset)

    val alreadyInMapF = {
      implicit val executionContext: ExecutionContext = directExecutionContext
      // `parTraverse` without limit is fine here because all these futures have already been spawned
      alreadyInMap.parTraverse { case (identifier, accumulator) =>
        accumulator.awaitInitialization.map(identifier -> _)
      }
    }

    val loadingF = {
      implicit val executionContext: ExecutionContext = directExecutionContext
      for {
        parties <- partiesF
        participants <- participantsF
      } yield (parties, participants)
    }

    val freshlyLoadedF = loadingF.transform(
      {
        case UnlessShutdown.Outcome((partyDigests, participantDigests)) =>
          val updatesAndAccumulatorsLoaded = toBeLoaded.map { case (identifier, accumulator) =>
            def initializeAccumulator[K, V](
                digests: Map[K, AcsDigestUpdate[K, V]],
                key: K,
            )(digestOf: V => RawDigest): Unit = {
              val digestUpdate = digests.getOrElse(
                key,
                AcsDigestStore.AcsDigestUpdate(AcsDigestStore.AcsDigest.empty(key, timepoint), None),
              )
              val acsDigest = digestUpdate.digestUpdate
              val rawDigest = acsDigest.digestO.map { v =>
                val snapshotTaken = SnapshotTaken(acsDigest.timepoint, digestUpdate.replacesOffset)
                digestOf(v) -> snapshotTaken
              }
              accumulator.initializeAccumulator(
                rawDigest,
                if (tracingMode == AcsDigestTracingMode.Full) acsDigest.trace else None,
                prettyIdentifier(identifier),
              )
            }

            identifier match {
              case party: PartyDigestIdentifier =>
                initializeAccumulator(partyDigests, party.partyAndOrder)(Predef.identity)

              case participant: ParticipantDigestIdentifier =>
                initializeAccumulator(participantDigests, participant.participantId)(_._1)
            }

            (identifier, accumulator)
          }
          UnlessShutdown.Outcome(updatesAndAccumulatorsLoaded)

        case AbortedDueToShutdown =>
          toBeLoaded.foreach { case (identifier, accumulator) =>
            accumulator.abort(Success(AbortedDueToShutdown), prettyIdentifier(identifier))
          }
          AbortedDueToShutdown
      },
      { ex =>
        toBeLoaded.foreach { case (identifier, accumulator) =>
          accumulator.abort(Failure(ex), prettyIdentifier(identifier))
        }
        ex
      },
    )

    val readyF = for {
      alreadyInMap <- alreadyInMapF
      freshlyLoaded <- freshlyLoadedF
    } yield (alreadyInMap ++ freshlyLoaded).toMap

    readyF
  }

  /** Computes the new LtHash changes needed for the given input: Only an
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor.AcsUpdate]] matters
    * because all other
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor.Classification]]s do
    * not create new LtHashes, but merely perform set operations among digests.
    */
  private def computeDigestChanges(input: EnsurePresentOutput): ComputeDigestChangesOutput =
    input.map(_.map { case classificationAndData @ (classification, _) =>
      classificationAndData.map(_.map { loadingF =>
        val computedO = classification match {
          case acsUpdate: AcsUpdate =>
            Some(computeDigestChangesFromAcsUpdate(acsUpdate))
          case _ => None
        }
        computedO -> loadingF
      })
    })

  private def computeDigestChangesFromAcsUpdate(acsUpdate: AcsUpdate): ComputedDigestChanges = {
    val deltas = DigestOps.computeDeltas(
      thisParticipant,
      acsUpdate,
      traceChanges = tracingMode != AcsDigestTracingMode.Disabled,
    )

    val stringInterning = stringInterningEval.value
    deltas.map {
      case DigestDelta.Party(partyAndOrder, digest, operation) =>
        val internedParty = stringInterning.party.internalize(partyAndOrder.party)
        ComputedDigestChange(
          PartyDigestIdentifier(internedParty, partyAndOrder.order),
          digest,
          operation,
        )
      case DigestDelta.Participant(participantId, digest, operation) =>
        val internedParticipant = stringInterning.participantId.internalize(participantId)
        ComputedDigestChange(
          ParticipantDigestIdentifier(internedParticipant),
          digest,
          operation,
        )
    }
  }

  /** The returned future completes when all accumulators have been loaded for this input.
    */
  private def joinLoading(
      input: ComputeDigestChangesOutput
  ): FutureUnlessShutdown[ApplyDigestChangesInput] = {
    implicit val executionContext: ExecutionContext = directExecutionContext
    input.traverse(_.traverse(_.traverse(_.traverse(_.sequence))))
  }

  /** Applies the digest changes for the `input` to the loaded accumulators */
  private def applyDigestChanges(
      input: ApplyDigestChangesInput
  ): DigestUpdateContext[UpdatedAccumulators] =
    input.map(_.map { case classificationAndData @ (classification, _) =>
      classificationAndData.map(_.map { case (computedDigestChangesO, accumulators) =>
        implicit val traceContext: TraceContext = input.traceContext
        applyDigestChanges2(classification, accumulators, computedDigestChangesO, input.timepoint)
      })
    })

  private def applyDigestChanges2(
      classification: Classification,
      accumulators: AccumulatingDigests,
      computedDigestChangesO: Option[ComputedDigestChanges],
      timepoint: Timepoint,
  )(implicit traceContext: TraceContext): UpdatedAccumulators = {
    val changedAccumulators = classification match {
      case _: AcsUpdate =>
        val digestChanges = computedDigestChangesO.getOrElse {
          ErrorUtil.invalidState(
            "Expected digest changes to be present for an ACS update classification, but they were not."
          )
        }
        doApplyDigestChanges(accumulators, digestChanges, timepoint)
      case hostingChange: PartyHostingChange =>
        val party = hostingChange.party
        val participant = hostingChange.participant
        val internedParty = stringInterning.party.internalize(party)
        val order = PartyOrder.orderFor(thisParticipant, participant)
        val partyIdentifier = PartyDigestIdentifier(internedParty, order)
        val partyDigest = accumulators
          .getOrElse(
            partyIdentifier,
            ErrorUtil.invalidState(
              s"Expected digest for $party being pre-loaded, but was not there"
            ),
          )
          .accumulator
        if (partyDigest.digest.isEmpty) Map.empty[DigestIdentifier, AccumulatingDigest]
        else {
          val participantIdentifier = ParticipantDigestIdentifier(
            stringInterning.participantId.internalize(participant)
          )
          val participantAccumulator = accumulators.getOrElse(
            participantIdentifier,
            ErrorUtil.invalidState(
              s"Expected digest for participant $participant being pre-loaded, but was not there"
            ),
          )
          val operation = hostingChange match {
            case _: PartyAddedToParticipant => DigestOperation.Add
            case _: PartyRemovedFromParticipant => DigestOperation.Remove
          }
          val description = operation match {
            case DigestOperation.Add => s"onboarded $party"
            case DigestOperation.Remove => s"offboarded $party"
          }
          val bulkPartyDigest = partyDigest.asBulk(description, operation)
          doApplyDigestChanges(
            Map(participantIdentifier -> participantAccumulator),
            immutable.Iterable(
              ComputedDigestChange(participantIdentifier, bulkPartyDigest, operation)
            ),
            timepoint,
          )
        }
      case onboarding: PartyOnboardingToParticipant =>
        ErrorUtil.invalidState(
          s"Expected no ACS digest update for onboarding change, but got $onboarding"
        )
    }
    UpdatedAccumulators(
      changedAccumulators,
      accumulators.keySet.map(_ -> PositiveInt.one).toMap,
      NonEmpty(Seq, traceContext),
    )
  }

  def doApplyDigestChanges(
      accumulators: AccumulatingDigests,
      digestChanges: ComputedDigestChanges,
      timepoint: Timepoint,
  )(implicit traceContext: TraceContext): AccumulatingDigests = {
    val updatedAccumulators = Map.newBuilder[DigestIdentifier, AccumulatingDigest]
    digestChanges.foreach { case ComputedDigestChange(identifier, digestChange, operation) =>
      val accumulator = accumulators.getOrElse(
        identifier,
        ErrorUtil.invalidState(
          s"Attempted to apply a digest change at $timepoint for an identifier that is not present: ${prettyIdentifier(identifier)}"
        ),
      )
      accumulator.updateDigest(digestChange, operation, timepoint)
      updatedAccumulators += (identifier -> accumulator)
    }
    updatedAccumulators.result()
  }

  private def makeUpdatedAccumulators(
      input: ApplyDigestChangesInput
  ): NonEmpty[Seq[DigestUpdateContext[UpdatedAccumulators]]] =
    NonEmpty(Seq, applyDigestChanges(input))

  private def fullAccumulator(
      acc: NonEmpty[Seq[DigestUpdateContext[UpdatedAccumulators]]]
  ): Boolean =
    acc.sizeIs > 1 ||
      acc.head1.value.getOption
        .forall(_._2.forall(_.loadedIdentifiers.sizeIs >= acsUpdateBatchSize))

  private def aggregateUpdatedAccumulators(
      acc: NonEmpty[Seq[DigestUpdateContext[UpdatedAccumulators]]],
      input: ApplyDigestChangesInput,
  ): NonEmpty[Seq[DigestUpdateContext[UpdatedAccumulators]]] = {
    implicit val traceContext: TraceContext = input.traceContext
    ErrorUtil.requireState(acc.sizeIs == 1, "Aggregation function called on full accumulator")
    val accState = acc.head1.value.getOption
      .flatMap(_._2)
      .getOrElse(
        ErrorUtil.invalidState(
          "Aggregation function called on empty element accumulator. Batch size limits should have prevented this."
        )
      )
    val applied = applyDigestChanges(input)
    applied
      .traverse {
        case CheckpointFence =>
          Left(acc :+ applied)
        case other =>
          other.traverse(_.traverse {
            case None =>
              // This case happens for non-checkpoint updates that do not change any of the digests,
              // for example when a party starts being onboarded to a participant.
              Left(acc :+ applied)
            case Some(updatedAccumulators) =>
              Right(Some(accState.conflateWith(updatedAccumulators)))
          })
      }
      .map(NonEmpty(Seq, _))
      .merge
  }

  /** Takes a copy of the raw digests of the updated accumulators and converts them to
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate]]s.
    */
  private def copySnapshot(input: ConflationOutput): CopySnapshotOutput = {
    val mergedTraceContext = input.value.getOption
      .flatMap(_._2)
      .map(_.traceContexts)
      .fold(input.traceContext)(TraceContext.ofBatch("DigestAccumulator")(_)(logger))
    input
      .map(_.map(_.map(_.map { conflatedUpdates =>
        val accumulators = conflatedUpdates.updatedAccumulators
        val snapshots = accumulators.map { case (identifier, accumulator) =>
          accumulator.snapshotForUpdate(identifier)
        }
          // Force evaluation of the map here so that the copying actually takes place in this stage of the flow.
          .toSeq
        snapshots -> conflatedUpdates.loadedIdentifiers
      })))
      .copy()(mergedTraceContext)
  }

  /** Persists the [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate]]s to
    * the stores
    */
  private def persistDigestUpdates(
      input: CopySnapshotOutput
  ): FutureUnlessShutdown[PersistDigestUpdatesOutput] = {
    implicit val traceContext: TraceContext = input.traceContext
    input.traverse(_.traverse(_.traverse(_.traverse { case (updates, usageCounters) =>
      val (participantUpdates, partyUpdates) = updates.partitionMap { update =>
        update
          .partitionMap(_.toEither)
          .leftMap(
            _.mapValue(rawDigest =>
              rawDigest -> Digest.hashDigest(rawDigest).getCryptographicEvidence
            )
          )
      }
      for {
        _ <- digestStore.party.upsertDigestUpdates(partyUpdates)
        _ <- digestStore.participant.upsertDigestUpdates(participantUpdates)
      } yield usageCounters
    })))
  }

  /** Persists checkpoints to the store. */
  private def persistCheckpoint(
      input: PersistDigestUpdatesOutput
  ): FutureUnlessShutdown[PersistDigestUpdatesOutput] = {
    implicit val traceContext: TraceContext = input.traceContext
    input.value match {
      case CheckpointFence =>
        implicit val executionContext = directExecutionContext
        digestStore
          .insertCheckpointTime(input.offset, input.recordTime)
          .map((_: Unit) => input)
      case other => FutureUnlessShutdown.pure(input)
    }
  }

  /** Deregisters the usages from [[digests]] and evicts unused accumulators. */
  private def deregister(input: PersistDigestUpdatesOutput): immutable.Iterable[CheckpointWritten] =
    input match {
      case context @ ProcessingContext(_, NotCheckpointFence(_, (_, usagesO))) =>
        usagesO.foreach { usages =>
          doDeregister(usages)(context.traceContext)
        }
        immutable.Iterable.empty
      case ProcessingContext(timepoint, CheckpointFence) =>
        immutable.Iterable(CheckpointWritten(timepoint))
    }

  private def doDeregister(
      identifiers: immutable.Iterable[(DigestIdentifier, PositiveInt)]
  )(implicit traceContext: TraceContext): Unit = {
    // Check that identifiers are distinct; otherwise usage counting will be broken.
    if (enableConsistencyChecks) {
      requireDistinct(identifiers)(identifier => s"Duplicate identifier: $identifier")
    }

    val evictionCount = identifiers.iterator.count { identifierWithMultiplicity =>
      val (identifier, multiplicity) = identifierWithMultiplicity
      digests
        .updateWith(identifier) {
          case Some(cad) =>
            val decremented = cad.decrementUsage(prettyIdentifier(identifier), multiplicity)
            // Immediately evict if the usage counter reaches zero.
            // Could instead enlist the entry in an LRU-style eviction list to keep it around for a bit
            if (decremented.usageCounter == 0) None else Some(decremented)
          case None =>
            throw new IllegalStateException(
              s"Attempted to deregister an identifier that is not present: ${prettyIdentifier(identifier)}"
            )
        }
        .isEmpty
    }

    logger.debug(s"Evicted $evictionCount entries")
  }

  private def stringInterning = stringInterningEval.value

  @VisibleForTesting
  def prettyIdentifier(identifier: DigestIdentifier): String = identifier match {
    case ParticipantDigestIdentifier(participantId) =>
      s"Participant(${stringInterning.participantId.externalize(participantId)})"
    case PartyDigestIdentifier(partyId, order) =>
      s"Party(${stringInterning.party.externalize(partyId)}, $order)"
  }

  @VisibleForTesting
  def digestsUsageCounters: Map[DigestIdentifier, Int] =
    digests.view.mapValues(_.usageCounter).toMap
}

object InMemoryDigestAccumulator {

  type AccumulatingDigests = Map[DigestIdentifier, AccumulatingDigest]
  type LoadedIdentifiers = Map[DigestIdentifier, PositiveInt]

  final case class ComputedDigestChange(
      digestIdentifier: DigestIdentifier,
      digest: TracedLtHash16Blake3,
      operation: DigestOperation,
  )
  type ComputedDigestChanges = immutable.Iterable[ComputedDigestChange]

  final case class UpdatedAccumulators(
      updatedAccumulators: AccumulatingDigests,
      loadedIdentifiers: LoadedIdentifiers,
      traceContexts: NonEmpty[Seq[TraceContext]],
  ) {
    def conflateWith(input: UpdatedAccumulators): UpdatedAccumulators = {
      val mergedAccumulators = this.updatedAccumulators ++ input.updatedAccumulators
      val mergedLoadedIdentifiers =
        MapsUtil.mergeWith(this.loadedIdentifiers, input.loadedIdentifiers)(_.+(_))
      val mergedTraceContexts = this.traceContexts ++ input.traceContexts
      UpdatedAccumulators(mergedAccumulators, mergedLoadedIdentifiers, mergedTraceContexts)
    }
  }

  type DigestUpdateSnapshot = immutable.Iterable[AcsDigestUpdate[DigestIdentifier, RawDigest]]

  type DigestUpdateContext[+A] = ProcessingContext[CheckpointFenceOr[(Classification, Option[A])]]
  type EnsurePresentOutput = DigestUpdateContext[FutureUnlessShutdown[AccumulatingDigests]]
  type ComputeDigestChangesOutput = DigestUpdateContext[
    (Option[ComputedDigestChanges], FutureUnlessShutdown[AccumulatingDigests])
  ]
  type ApplyDigestChangesInput =
    DigestUpdateContext[(Option[ComputedDigestChanges], AccumulatingDigests)]
  type ConflationOutput = DigestUpdateContext[UpdatedAccumulators]
  type CopySnapshotOutput = DigestUpdateContext[(DigestUpdateSnapshot, LoadedIdentifiers)]
  type PersistDigestUpdatesOutput = DigestUpdateContext[LoadedIdentifiers]

  private def requireDistinct[A](vals: immutable.Iterable[A])(message: A => String): Unit = {
    val set = scala.collection.mutable.Set[A]()
    vals.foreach { v =>
      if (set.contains(v)) throw new IllegalArgumentException(message(v))
      else set += v
    }
  }

  sealed trait DigestIdentifier extends Product with Serializable {
    def toEither: Either[InternedParticipantId, PartyAndOrder[InternedPartyId]]
  }
  final case class ParticipantDigestIdentifier(participantId: InternedParticipantId)
      extends DigestIdentifier {
    override def toEither: Either[InternedParticipantId, PartyAndOrder[InternedPartyId]] = Left(
      participantId
    )
  }
  final case class PartyDigestIdentifier(partyId: InternedPartyId, order: PartyOrder)
      extends DigestIdentifier {
    def partyAndOrder: PartyAndOrder[InternedPartyId] = PartyAndOrder(partyId, order)
    override def toEither: Either[InternedParticipantId, PartyAndOrder[InternedPartyId]] =
      Right(partyAndOrder)
  }
  object DigestIdentifier {
    implicit val orderingDigestIdentifier: Ordering[DigestIdentifier] =
      Ordering.by[DigestIdentifier, (Boolean, Int)] {
        case ParticipantDigestIdentifier(participantId) => (true, participantId)
        case PartyDigestIdentifier(partyId, order) =>
          (false, PartyAndOrder.encodePartyAndOrder(PartyAndOrder(partyId, order)))
      }
  }

  /** @param usageCounter
    *   Counts how many elements are in the pipeline that potentially need access to the
    *   accumulator. Always non-negative.
    */
  private final case class CountingAccumulatingDigest(
      usageCounter: Int,
      accumulator: AccumulatingDigest,
  ) {
    def incrementUsage(identifier: => String, delta: PositiveInt): CountingAccumulatingDigest =
      try {
        this.copy(usageCounter = Math.addExact(usageCounter, delta.value))
      } catch {
        case _: ArithmeticException =>
          throw new ArithmeticException(
            s"Usage counter overflow for $identifier ($usageCounter + $delta)."
          )
      }

    def decrementUsage(identifier: => String, delta: PositiveInt): CountingAccumulatingDigest = {
      val newUsageCounter = usageCounter - delta.value
      if (newUsageCounter < 0) {
        throw new ArithmeticException(
          s"Usage counter underflow for $identifier ($usageCounter - $delta)."
        )
      }
      this.copy(usageCounter = usageCounter - delta.value)
    }
  }

  private object CountingAccumulatingDigest {
    def initial(timepoint: Timepoint): CountingAccumulatingDigest =
      CountingAccumulatingDigest(usageCounter = 1, new AccumulatingDigest(timepoint))
  }

  final class AccumulatingDigest(initialTimepoint: Timepoint) {

    private[this] val state: AtomicReference[AccumulatingDigestState] =
      new AtomicReference[AccumulatingDigestState](
        AccumulatingDigestState.Loading(Promise[UnlessShutdown[AccumulatingDigest]]())
      )

    private[this] val accumulatorInternal: TracedLtHash16Blake3Impl = TracedLtHash16Blake3Impl.empty

    def accumulator: TracedLtHash16Blake3 = accumulatorInternal

    // Invariant: lastSnapshotAtInternal <= Some(currentRecordTimeInternal)
    // Invariant: Never decreases
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private[this] var lastSnapshotAtInternal: Option[SnapshotTaken] = None

    // Invariant: Never decreases
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private[this] var currentTimepointInternal: Timepoint = initialTimepoint

    def isInitialized: Boolean = state.get == AccumulatingDigestState.Loaded

    def awaitInitialization: FutureUnlessShutdown[AccumulatingDigest] = state.get match {
      case AccumulatingDigestState.Loading(promise) => FutureUnlessShutdown(promise.future)
      case AccumulatingDigestState.Initializing(future) => future
      case AccumulatingDigestState.Loaded => FutureUnlessShutdown.pure(this)
      case AccumulatingDigestState.Aborted => FutureUnlessShutdown.abortedDueToShutdown
    }

    def initializeAccumulator(
        initialDigest: Option[(RawDigest, SnapshotTaken)],
        trace: Option[AcsDigestTrace],
        identifier: => String,
    ): Unit = {
      def getPromise(
          previous: AccumulatingDigestState
      ): Promise[UnlessShutdown[AccumulatingDigest]] = previous match {
        case AccumulatingDigestState.Loading(promise) => promise
        case _ =>
          throw new IllegalStateException(
            s"Accumulator state for $identifier is being set multiple times!"
          )
      }

      val initializationPromise = initialDigest match {
        case Some((rawDigest, snapshotTaken)) =>
          val currentTimepoint = currentTimepointInternal
          val timepoint = snapshotTaken.timepoint
          if (timepoint > currentTimepoint) {
            throw new IllegalStateException(
              s"Timepoint $timepoint of digest for $identifier is after the current timepoint $currentTimepoint."
            )
          }
          val previous = state.getAndUpdate {
            case AccumulatingDigestState.Loading(initializedPromise) =>
              AccumulatingDigestState.Initializing(FutureUnlessShutdown(initializedPromise.future))
            case other => other
          }
          val promise = getPromise(previous)
          accumulatorInternal.digest.setBytes(rawDigest)
          trace.foreach { items =>
            accumulatorInternal.addAll(items.traces)
          }
          lastSnapshotAtInternal = Some(snapshotTaken)
          state.set(AccumulatingDigestState.Loaded)
          promise
        case None =>
          // If we don't have to write to the accumulator, we can skip the intermediate stage of `Initializing`.
          val previous = state.getAndUpdate {
            case _: AccumulatingDigestState.Loading => AccumulatingDigestState.Loaded
            case other => other
          }
          getPromise(previous)
      }

      val success = initializationPromise.trySuccess(UnlessShutdown.Outcome(this))
      // Check whether the initialization promise has previously been completed successfully.
      // Tolerate earlier completions due to shutdown or exceptions.
      if (!success) {
        val previousCompletion = initializationPromise.future.value.getOrElse(
          throw new IllegalStateException(
            "Failed to complete promise, but promise remains uncompleted"
          )
        )
        previousCompletion.foreach(
          _.foreach(_ =>
            throw new IllegalStateException(
              s"Attempted to successfully complete the accumulator initialization promise for $identifier multiple times"
            )
          )
        )
      }
    }

    def abort(reason: Try[UnlessShutdown[Nothing]], identifier: => String): Unit = {
      val previous = state.getAndUpdate {
        case _: AccumulatingDigestState.Loading => AccumulatingDigestState.Aborted
        case _: AccumulatingDigestState.Initializing | AccumulatingDigestState.Loaded =>
          throw new IllegalStateException(
            s"Accumulator state for $identifier is both set and aborted"
          )
        case AccumulatingDigestState.Aborted => AccumulatingDigestState.Aborted
      }
      previous match {
        case AccumulatingDigestState.Loading(initializationPromise) =>
          initializationPromise.tryComplete(reason).discard[Boolean]
        case _ => ()
      }
    }

    def updateDigest(
        digestChange: TracedLtHash16Blake3,
        operation: DigestOperation,
        timepoint: Timepoint,
    ): Unit = {
      val current = currentTimepointInternal
      require(
        current <= timepoint,
        s"Timepoint is at $current and cannot go backwards to $timepoint.",
      )
      operation match {
        case DigestOperation.Add =>
          accumulatorInternal.union(digestChange)
        case DigestOperation.Remove =>
          accumulatorInternal.removeAll(digestChange)
      }
      currentTimepointInternal = timepoint
    }

    def snapshotForUpdate[K](key: K): AcsDigestUpdate[K, RawDigest] = {
      val timepoint = currentTimepointInternal
      val lastSnapshot = lastSnapshotAtInternal
      val replacementOffset = lastSnapshot.flatMap { snapshotTaken =>
        if (snapshotTaken.timepoint < timepoint) Some(snapshotTaken.timepoint.offset)
        else snapshotTaken.replacementOffset
      }
      if (lastSnapshot.forall(_.timepoint < timepoint))
        lastSnapshotAtInternal = Some(SnapshotTaken(timepoint, replacementOffset))
      val rawDigest =
        Option.when(!accumulatorInternal.digest.isEmpty)(accumulatorInternal.digest.getByteString)
      val trace = accumulatorInternal.trace
      val acsDigest = AcsDigest(key, timepoint, rawDigest, trace)
      AcsDigestUpdate(acsDigest, replacementOffset)
    }

    def snapshot: RawDigest = accumulatorInternal.digest.getByteString

    override def toString: String = {
      val sb = new StringBuilder
      sb.addAll("AccumulatingDigest(")
      state.get() match {
        case _: AccumulatingDigestState.Loading => sb.addAll("Loading")
        case _: AccumulatingDigestState.Initializing => sb.addAll("Initializing")
        case AccumulatingDigestState.Aborted => sb.addAll("Aborted")
        case AccumulatingDigestState.Loaded =>
          sb.addAll("current=").addAll(currentTimepointInternal.offset.toString).discard
          lastSnapshotAtInternal.foreach { snapshotAt =>
            sb.addAll(", lastSnapshotAt=").addAll(snapshotAt.toString)
          }
      }
      sb.addAll(")")
      sb.result()
    }
  }

  final case class SnapshotTaken(
      timepoint: Timepoint,
      replacementOffset: Option[Offset],
  )

  private sealed trait AccumulatingDigestState extends Product with Serializable

  private object AccumulatingDigestState {

    /** The digest is being loaded from the store
      * @param initializationPromise
      *   Promise to be completed once the digest has been loaded from the store and the accumulator
      *   has been initialized.
      */
    final case class Loading(
        initializationPromise: Promise[UnlessShutdown[AccumulatingDigest]]
    ) extends AccumulatingDigestState

    /** The digest has been loaded from the store and is being written into the accumulator
      *
      * @param initializationFuture
      *   Future to complete once initialization has finished
      */
    final case class Initializing(
        initializationFuture: FutureUnlessShutdown[AccumulatingDigest]
    ) extends AccumulatingDigestState

    /** The accumulator can be used */
    final case object Loaded extends AccumulatingDigestState

    /** The loading of the digest has been aborted. The accumulator cannot be used. */
    final case object Aborted extends AccumulatingDigestState
  }

  private implicit class FlowBlockSyntax[A, B, Mat](val flow: Flow[A, DigestUpdateContext[B], Mat])
      extends AnyVal {
    def withBlocker(
        blockerO: Option[ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit]]
    )(implicit executionContext: ExecutionContext): Flow[A, DigestUpdateContext[B], Mat] =
      blockerO match {
        case None => flow
        case Some(blocker) =>
          flow.mapAsync(parallelism = 1) { input =>
            blocker(input.map(_.map(_._1))).map(_ => input)
          }
      }
  }
}
