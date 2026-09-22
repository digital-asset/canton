// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.option.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  ContractChangeBatch,
  NotCheckpointFence,
  ProcessingContext,
}
import com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator.{
  DigestIdentifier,
  ParticipantDigestIdentifier,
  PartyDigestIdentifier,
}
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.digest.DigestOps
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  CheckpointType,
  InternedParticipantId,
  RawDigest,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.CombinedKillSwitch
import com.digitalasset.canton.util.PekkoUtil.syntax.pekkoUtilSyntaxForFlowOpsSource
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.digitalasset.canton.{InternedPartyId, LfPartyId}
import com.digitalasset.nonempty.NonEmptyUtil
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class DigestConsistencyCheckProcessorImpl(
    override val thisParticipantId: ParticipantId,
    override val synchronizerId: SynchronizerId,
    acsCommitmentConfig: AcsCommitmentConfig,
    digestAccumulatorStoreFactory: () => InMemoryAcsDigestStore,
    digestAccumulatorFactory: InMemoryAcsDigestStore => DigestAccumulator,
    protected override val acsDigestStore: AcsDigestStore,
    indexService: InternalIndexService,
    digestProcessorTopologyLookup: DigestProcessorTopologyLookup,
    stringInterning: StringInterning,
    enableAdditionalConsistencyChecks: Boolean,
    private[canton] override val metrics: CommitmentMetrics,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    mat: Materializer,
) extends DigestConsistencyCheckProcessor {

  import DigestConsistencyCheckProcessorImpl.*

  private val startTimestampRef = new AtomicReference[Option[CantonTimestamp]](None)

  private val partyDigestJournal: AcsDigestStore.DigestJournal[InternedPartyId] =
    acsDigestStore.party

  private val participantDigestJournal: AcsDigestStore.DigestJournal[InternedParticipantId] =
    acsDigestStore.participant

  private val counterpartyBatchSize: Int = acsCommitmentConfig.counterpartyBatchSize.unwrap

  private val journalSnapshotQueryLimit =
    acsCommitmentConfig.consistencyCheck.journalSnapshotQueryLimit.unwrap

  private val acsRetrievalConfigOverrides = ActiveContractsServiceStreamsConfigOverrides(
    maxParallelActiveIdQueries = acsCommitmentConfig.maxParallelActiveIdQueries.unwrap,
    maxParallelPayloadCreateQueries = acsCommitmentConfig.maxParallelPayloadCreateQueries.unwrap,
  )

  type ParticipantDigestMap = Map[InternedParticipantId, RawDigest]

  override def startTimestamp: Option[CantonTimestamp] = startTimestampRef.get()

  def runConsistencyCheck(
      timepoint: Timepoint,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): (KillSwitch, Future[Unit]) = {
    logger.info(s"Starting a consistency check for timepoint $timepoint")
    startTimestampRef.set(timepoint.recordTime.some)

    val ((firstKillSwitch, allCounterpartiesF), participantDigestsF) =
      partyDigestMissingAndMismatchedInconsistencies(timepoint, topologySnapshot)
        .to(
          Sink.foreach(logInconsistency)
        )
        .run()

    val (secondKillSwitch, doneF) = Source
      .futureSource(allCounterpartiesF.map { allCounterparties =>
        unexpectedPartyDigestsInStore(allCounterparties, timepoint.offset)
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .concat(
        Source
          .futureSource(
            // The number of participants should be limited, so we can get away with calling Source from a collection
            for {
              participantDigests <- participantDigestsF
              inconsistencies <- participantDigestInconsistencies(participantDigests, timepoint)
            } yield Source(inconsistencies)
          )
      )
      .toMat(
        Sink.foreach(logInconsistency)
      )(Keep.both)
      .run()

    (
      new CombinedKillSwitch(firstKillSwitch, secondKillSwitch),
      doneF.map { _ =>
        logger.info(s"Finished a consistency check for timepoint $timepoint")
      },
    )
  }

  private def logInconsistency(inconsistency: DigestInconsistency)(implicit
      traceContext: TraceContext
  ): Unit = {
    val identifiers =
      inconsistency.digestIdentifiers.map(DigestIdentifier.prettyIdentifier(_, stringInterning))

    logger.warn(
      s"Found the digest inconsistency: $inconsistency with identifiers $identifiers."
    )
  }

  private[commitment] def participantDigestInconsistencies(
      recalculatedParticipantDigests: ParticipantDigestMap,
      timepoint: Timepoint,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[DigestInconsistency]] =
    for {
      previouslyStoredParticipantDigests <- loadAllParticipantDigests(
        participantDigestJournal,
        timepoint,
      )
    } yield {
      detectInconsistencies(
        recalculatedParticipantDigests,
        previouslyStoredParticipantDigests,
      )(ParticipantDigestIdentifier.apply)
    }

  private[commitment] def unexpectedPartyDigestsInStore(
      allExpectedParties: Set[InternedPartyId],
      offset: Offset,
  )(implicit
      tc: TraceContext
  ): Source[DigestInconsistency, NotUsed] =
    AcsDigestStore.DigestJournal
      .sourceFromJournalSnapshot[InternedPartyId, Option[DigestInconsistency]](
        partyDigestJournal
      )(offset, journalSnapshotQueryLimit) { digestUpdates =>
        val partyIds = extractPartyIdsWithNonEmptyDigests(digestUpdates)
        val unexpectedPartyIds = partyIds.diff(allExpectedParties)

        Option.when(unexpectedPartyIds.nonEmpty)(
          UnexpectedDigestsInStore(
            unexpectedPartyIds.map(PartyDigestIdentifier.apply)
          )
        )
      }
      .mapConcat(identity)

  private def extractPartyIdsWithNonEmptyDigests(
      updates: Iterable[AcsDigestUpdate[InternedPartyId]]
  ): Set[InternedPartyId] =
    updates.collect { case AcsDigestUpdate(AcsDigest(internedPartyId, _, _, Some(_), _), _) =>
      internedPartyId
    }.toSet

  private[commitment] def partyDigestMissingAndMismatchedInconsistencies(
      timepoint: Timepoint,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): Source[
    DigestInconsistency,
    ((KillSwitch, Future[Set[InternedPartyId]]), Future[ParticipantDigestMap]),
  ] =
    counterpartyBatches(timepoint)
      .alsoToMat(Sink.fold(Set.empty[InternedPartyId])(_ ++ _.map(internedPartyId)))(Keep.both)
      .mapAsyncAndDrainUS(1) { counterparties =>
        val counterpartiesSet = counterparties.toSet

        val consistencyCheckStore = digestAccumulatorStoreFactory()
        val digestAccumulator = digestAccumulatorFactory(consistencyCheckStore)

        FutureUnlessShutdown
          .outcomeF(
            contractChangeBatches(counterpartiesSet, timepoint, topologySnapshot)
              .via(
                digestAccumulator.flow()
              )
              .runWith(Sink.ignore)
          )
          .flatMap { _ =>
            val internedCounterparties = counterpartiesSet.map(internedPartyId)
            for {
              recomputed <- consistencyCheckStore.party.bulkLookup(
                internedCounterparties,
                timepoint.offset,
              )
              previouslyStored <- acsDigestStore.party.bulkLookup(
                internedCounterparties,
                timepoint.offset,
              )
              participantDigests <-
                FutureUnlessShutdown
                  .outcomeF(
                    loadAllParticipantDigests(consistencyCheckStore.participant, timepoint)
                  )
            } yield (
              detectInconsistencies(
                extractNonEmptyDigests(recomputed),
                extractNonEmptyDigests(previouslyStored),
              )(
                PartyDigestIdentifier.apply
              ),
              participantDigests,
            )
          }
      }
      .alsoToMat(
        Sink.fold(Map.empty[InternedParticipantId, RawDigest]) {
          case (previousParticipantDigests, (_, participantDigests)) =>
            mergeDigestMaps(previousParticipantDigests, participantDigests)
        }
      )(Keep.both)
      .map(_._1)
      .mapConcat(identity)

  private def loadAllParticipantDigests(
      digestJournal: AcsDigestStore.DigestJournal[InternedParticipantId],
      timepoint: Timepoint,
  )(implicit
      traceContext: TraceContext
  ): Future[ParticipantDigestMap] =
    AcsDigestStore.DigestJournal
      .sourceFromJournalSnapshot[InternedParticipantId, ParticipantDigestMap](digestJournal)(
        timepoint.offset,
        journalSnapshotQueryLimit,
      ) { updates =>
        updates.collect {
          case AcsDigestUpdate(AcsDigest(participantId, _, _, Some(rawDigest), _), _) =>
            participantId -> rawDigest
        }.toMap
      }
      .runWith(
        Sink.fold[ParticipantDigestMap, ParticipantDigestMap](Map.empty)(
          ensureUniqueAndMergeParticipantDigestMaps
        )
      )

  private def detectInconsistencies[K](
      recomputedDigests: Map[K, RawDigest],
      previouslyStoredDigests: Map[K, RawDigest],
  )(makeIdentifier: K => DigestIdentifier): Seq[DigestInconsistency] = {
    val missingKeysInStore = recomputedDigests.keySet.diff(previouslyStoredDigests.keySet)
    val unexpectedKeysInStore = previouslyStoredDigests.keySet.diff(recomputedDigests.keySet)

    val keysWithMismatchedDigests = recomputedDigests
      .filter { case (key, _) =>
        previouslyStoredDigests.isDefinedAt(key)
      }
      .flatMap { case (key, recomputedDigest) =>
        val previouslyStoredDigest0 = previouslyStoredDigests.get(key)

        if (previouslyStoredDigest0.contains(recomputedDigest)) None
        else Some(key)
      }
      .toSeq

    val missingDigestsInconsistency0: Option[DigestInconsistency] =
      Option.when(missingKeysInStore.nonEmpty)(
        MissingDigestsInStore(missingKeysInStore.map(makeIdentifier))
      )

    val unexpectedDigestsInconsistency0: Option[DigestInconsistency] =
      Option.when(unexpectedKeysInStore.nonEmpty)(
        UnexpectedDigestsInStore(unexpectedKeysInStore.map(makeIdentifier))
      )

    val digestValueMismatches = keysWithMismatchedDigests.map { party =>
      DigestValueMismatch(makeIdentifier(party))
    }

    digestValueMismatches ++ missingDigestsInconsistency0 ++ unexpectedDigestsInconsistency0
  }

  private[commitment] def extractNonEmptyDigests[K](
      digests: Map[K, AcsDigestUpdate[K]]
  ): Map[K, RawDigest] = digests.collect {
    case (key, AcsDigestUpdate(AcsDigest(_, _, _, Some(digest), _), _)) =>
      key -> digest
  }

  private[commitment] def counterpartyBatches(
      timepoint: Timepoint
  )(implicit
      traceContext: TraceContext
  ): Source[Seq[LfPartyId], KillSwitch] =
    indexService
      .counterParties(
        synchronizerId = synchronizerId,
        activeAt = timepoint.offset,
        party = None,
        configOverrides = acsRetrievalConfigOverrides,
      )
      .viaMat(KillSwitches.single)(Keep.right)
      .grouped(counterpartyBatchSize)

  private[commitment] def contractChangeBatches(
      counterpartiesSet: Set[LfPartyId],
      timepoint: Timepoint,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): Source[ProcessingContext[NotCheckpointFence[ContractChangeBatch]], NotUsed] =
    indexService
      .acs(
        synchronizerId = synchronizerId,
        activeAt = timepoint.offset,
        stakeholders1 = counterpartiesSet,
        stakeholders2 = Set.empty,
        configOverrides = acsRetrievalConfigOverrides,
      )
      .grouped(acsCommitmentConfig.contractChangeClassificationBatchSize.unwrap)
      .mapAsyncAndDrainUS(
        acsCommitmentConfig.contractChangeClassificationParallelism.unwrap
      ) { activeContractsOfCounterparties =>
        getContractChangeBatches(
          counterpartiesSet,
          activeContractsOfCounterparties,
          topologySnapshot,
          timepoint,
          enableAdditionalConsistencyChecks,
        )
      }

  private[commitment] def ensureUniqueAndMergeParticipantDigestMaps(
      digestMap1: ParticipantDigestMap,
      digestMap2: ParticipantDigestMap,
  )(implicit traceContext: TraceContext): ParticipantDigestMap = {
    val repeatedKeys =
      digestMap1.keySet.intersect(digestMap2.keySet).map(stringInterning.participantId.externalize)

    if (repeatedKeys.nonEmpty) {
      ErrorUtil.invalidState(
        s"Unexpected repeated participant digests for participants $repeatedKeys"
      )
    }

    digestMap1 ++ digestMap2
  }

  private def internedPartyId(partyId: LfPartyId): InternedPartyId =
    stringInterning.party.internalize(partyId)

  override protected def startPipelineInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] = {

    // TODO(#35929) Change the filter back to all after fixing all the tests and the logic
    val checkpointsFilter = Some(
      // We know the list (all - party hosting) is not empty
      NonEmptyUtil.fromUnsafe(CheckpointType.all - CheckpointType.PartyHostingChange)
    )

    for {
      latestCheckpointO <- acsDigestStore.latestCheckpointUpTo(Offset.MaxValue, checkpointsFilter)
      topologySnapshotO = latestCheckpointO.flatMap { latestCheckpoint =>
        digestProcessorTopologyLookup
          .topologySnapshotForReinitialization(
            synchronizerId,
            latestCheckpoint.recordTime,
          )
      }
    } yield {
      (latestCheckpointO, topologySnapshotO) match {
        case (Some(latestCheckpoint), Some(topologySnapshot)) =>
          runConsistencyCheck(latestCheckpoint.timepoint, topologySnapshot)
        case _ => (PekkoUtil.noOpKillSwitch, Future.successful(()))
      }
    }
  }
}

object DigestConsistencyCheckProcessorImpl {

  private[commitment] def mergeDigestMaps[K](
      digestMap1: Map[K, RawDigest],
      digestMap2: Map[K, RawDigest],
  ): Map[K, RawDigest] =
    MapsUtil
      .mergeWith(digestMap1, digestMap2) { case (digest1, digest2) =>
        DigestOps
          .combineDigests(
            Seq(digest1, digest2).map { digest =>
              TracedLtHash16Blake3(LtHash16Blake3.tryCreate(digest), Seq.empty)
            }
          )
          .digest
          .getByteString

      }

  sealed trait DigestInconsistency extends Product with Serializable {
    def digestIdentifiers: Set[DigestIdentifier]
  }

  final case class MissingDigestsInStore(missingKeys: Set[DigestIdentifier])
      extends DigestInconsistency {
    override def digestIdentifiers: Set[DigestIdentifier] = missingKeys
  }

  final case class UnexpectedDigestsInStore(unexpectedKeys: Set[DigestIdentifier])
      extends DigestInconsistency {
    override def digestIdentifiers: Set[DigestIdentifier] = unexpectedKeys
  }

  final case class DigestValueMismatch(identifier: DigestIdentifier) extends DigestInconsistency {
    override def digestIdentifiers: Set[DigestIdentifier] = Set(identifier)
  }
}
