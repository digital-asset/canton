// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, NonNegativeDuration}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.event.{AcsChange, RecordTime}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  InFlightSubmission,
  SequencedSubmission,
  TestSubmissionTrackingData,
  UnsequencedSubmission,
}
import com.digitalasset.canton.participant.pruning
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  CommitmentSnapshot,
  CommitmentsPruningBound,
  RunningCommitments,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentStore,
  InMemoryActiveContractStore,
  InMemoryInFlightSubmissionStore,
  InMemoryRequestJournalStore,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  DefaultOpenEnvelope,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import java.time.{Duration as JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
sealed trait AcsCommitmentProcessorBaseTest
    extends BaseTest
    with SortedReconciliationIntervalsHelpers {

  protected val interval = PositiveSeconds.ofSeconds(5)
  protected val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))
  protected val localId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain")
  )
  protected val remoteId1 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant1::domain")
  )
  protected val remoteId2 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )
  protected val remoteId3 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::domain")
  )

  protected val List(alice, bob, carol) =
    List("Alice::1", "Bob::2", "Carol::3").map(LfPartyId.assertFromString)

  protected val topology = Map(
    localId -> Set(alice),
    remoteId1 -> Set(bob),
    remoteId2 -> Set(carol),
  )

  protected def ts(i: Int): CantonTimestampSecond = CantonTimestampSecond.ofEpochSecond(i.longValue)

  protected def toc(timestamp: Int, requestCounter: Int = 0): TimeOfChange =
    TimeOfChange(RequestCounter(requestCounter), ts(timestamp).forgetRefinement)

  protected def mkChangeIdHash(index: Int) = ChangeIdHash(DefaultDamlValues.lfhash(index))

  protected def acsSetup(
      lifespan: Map[LfContractId, (CantonTimestamp, CantonTimestamp)]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[ActiveContractSnapshot] = {
    val acs = new InMemoryActiveContractStore(loggerFactory)
    lifespan.toList
      .parTraverse_ { case (cid, (createdTs, archivedTs)) =>
        for {
          _ <- acs.createContract(cid, TimeOfChange(RequestCounter(0), createdTs)).value
          _ <- acs.archiveContract(cid, TimeOfChange(RequestCounter(0), archivedTs)).value
        } yield ()
      }
      .map(_ => acs)
  }

  protected def cryptoSetup(
      owner: ParticipantId,
      topology: Map[ParticipantId, Set[LfPartyId]],
  ): SyncCryptoClient[DomainSnapshotSyncCryptoApi] = {
    val topologyWithPermissions =
      topology.fmap(_.map(p => (p, ParticipantPermission.Submission)).toMap)
    TestingTopology().withReversedTopology(topologyWithPermissions).build().forOwnerAndDomain(owner)
  }

  protected def changesAtToc(
      contractSetup: Map[LfContractId, (Set[LfPartyId], TimeOfChange, TimeOfChange)]
  )(toc: TimeOfChange): (CantonTimestamp, RequestCounter, AcsChange) = {
    (
      toc.timestamp,
      toc.rc,
      contractSetup.foldLeft(AcsChange.empty) {
        case (acsChange, (cid, (stkhs, creationToc, archivalToc))) =>
          val metadata = ContractMetadata.tryCreate(Set.empty, stkhs, None)
          AcsChange(
            deactivations = acsChange.deactivations ++ (if (archivalToc == toc)
                                                          Map(cid -> withTestHash(stkhs))
                                                        else Map.empty),
            activations = acsChange.activations ++ (if (creationToc == toc)
                                                      Map(cid -> withTestHash(metadata))
                                                    else Map.empty),
          )
      },
    )
  }

  // Create the processor, but return the changes instead of publishing them, such that the user can decide when
  // to publish
  protected def testSetupDontPublish(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[LfContractId, (Set[LfPartyId], TimeOfChange, TimeOfChange)],
      topology: Map[ParticipantId, Set[LfPartyId]],
      killSwitch: => Unit = (),
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
  )(implicit ec: ExecutionContext): (
      AcsCommitmentProcessor,
      AcsCommitmentStore,
      SequencerClient,
      List[(CantonTimestamp, RequestCounter, AcsChange)],
  ) = {
    val domainCrypto = cryptoSetup(localId, topology)

    val sequencerClient = mock[SequencerClient]
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[SendCallback],
      )(anyTraceContext)
    )
      .thenReturn(EitherT.rightT[Future, SendAsyncClientError](()))

    val changeTimes =
      (timeProofs.map(ts => TimeOfChange(RequestCounter(0), ts)) ++ contractSetup.values.toList
        .flatMap { case (_, creationTs, archivalTs) =>
          List(creationTs, archivalTs)
        }).distinct.sorted
    val changes = changeTimes.map(changesAtToc(contractSetup))
    val store = optCommitmentStore.getOrElse(new InMemoryAcsCommitmentStore(loggerFactory))

    val sortedReconciliationIntervalsProvider =
      overrideDefaultSortedReconciliationIntervalsProvider.getOrElse {
        constantSortedReconciliationIntervalsProvider(interval)
      }

    val acsCommitmentProcessor = new AcsCommitmentProcessor(
      domainId,
      localId,
      sequencerClient,
      domainCrypto,
      sortedReconciliationIntervalsProvider,
      store,
      (_, _) => FutureUnlessShutdown.unit,
      killSwitch,
      ParticipantTestMetrics.pruning,
      testedProtocolVersion,
      DefaultProcessingTimeouts.testing
        .copy(storageMaxRetryInterval = NonNegativeDuration.tryFromDuration(1.millisecond)),
      loggerFactory,
    )

    (acsCommitmentProcessor, store, sequencerClient, changes)
  }

  protected def testSetup(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[LfContractId, (Set[LfPartyId], TimeOfChange, TimeOfChange)],
      topology: Map[ParticipantId, Set[LfPartyId]],
      killSwitch: => Unit = (),
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
  )(implicit
      ec: ExecutionContext
  ): (AcsCommitmentProcessor, AcsCommitmentStore, SequencerClient) = {

    val (acsCommitmentProcessor, store, sequencerClient, changes) =
      testSetupDontPublish(
        timeProofs,
        contractSetup,
        topology,
        killSwitch,
        optCommitmentStore,
        overrideDefaultSortedReconciliationIntervalsProvider,
      )

    changes.foreach { case (ts, rc, acsChange) =>
      acsCommitmentProcessor.publish(RecordTime(ts, rc.v), acsChange)
    }
    (acsCommitmentProcessor, store, sequencerClient)
  }

  val testHash = ExampleTransactionFactory.lfHash(0)

  protected def withTestHash[A] = WithContractHash[A](_, testHash)

  protected def rt(timestamp: Int, tieBreaker: Int) =
    RecordTime(ts(timestamp).forgetRefinement, tieBreaker.toLong)

  val coid = (txId, discriminator) => ExampleTransactionFactory.suffixedId(txId, discriminator)
}

class AcsCommitmentProcessorTest extends AsyncWordSpec with AcsCommitmentProcessorBaseTest {
  // This is duplicating the internal logic of the commitment computation, but I don't have a better solution at the moment
  // if we want to test whether commitment buffering works
  // Also assumes that all the contract IDs in the list have the same stakeholders
  private def commitment(cids: List[LfContractId]): AcsCommitment.CommitmentType = {
    val h = LtHash16()
    cids.foreach { cid =>
      h.add((testHash.bytes.toByteString concat cid.encodeDeterministically).toByteArray)
    }
    val doubleH = LtHash16()
    doubleH.add(h.get())
    doubleH.getByteString()
  }

  private def commitmentMsg(
      params: (ParticipantId, List[LfContractId], CantonTimestampSecond, CantonTimestampSecond)
  ): Future[SignedProtocolMessage[AcsCommitment]] = {
    val (remote, cids, fromExclusive, toInclusive) = params

    val crypto =
      TestingTopology().withParticipants(remote).build().forOwnerAndDomain(remote)
    val cmt = commitment(cids)
    val snapshotF = crypto.snapshot(CantonTimestamp.Epoch)
    val period =
      CommitmentPeriod
        .create(fromExclusive.forgetRefinement, toInclusive.forgetRefinement, interval)
        .value
    val payload =
      AcsCommitment.create(domainId, remote, localId, period, cmt, testedProtocolVersion)

    snapshotF.flatMap { snapshot =>
      SignedProtocolMessage.tryCreate(payload, snapshot, testedProtocolVersion)
    }
  }

  "AcsCommitmentProcessor.safeToPrune" must {
    "compute timestamp with no clean replay timestamp (no noOutstandingCommitment tick known)" in {
      val longInterval = PositiveSeconds.ofDays(100)
      for {
        res <- AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(CantonTimestamp.MinValue),
          commitmentsPruningBound =
            CommitmentsPruningBound.Outstanding(_ => Future.successful(None)),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
        )
      } yield res shouldBe None
    }

    "compute safeToPrune timestamp with no clean replay timestamp" in {
      val longInterval = PositiveSeconds.ofDays(100)
      for {
        res <- AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(CantonTimestamp.MinValue),
          commitmentsPruningBound = CommitmentsPruningBound.Outstanding(_ =>
            Future.successful(Some(CantonTimestamp.MinValue))
          ),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
        )
      } yield res shouldBe Some(CantonTimestampSecond.MinValue)
    }

    "take checkForOutstandingCommitments flag into account" in {
      val longInterval = PositiveSeconds.ofDays(100)
      val now = CantonTimestamp.now()

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(longInterval)

      def safeToPrune(
          checkForOutstandingCommitments: Boolean
      ): Future[Option[CantonTimestampSecond]] = {
        val noOutstandingCommitmentsF: CantonTimestamp => Future[Some[CantonTimestamp]] =
          _ => Future.successful(Some(CantonTimestamp.MinValue))
        val lastComputedAndSentF = Future.successful(Some(now))

        AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(now),
          commitmentsPruningBound =
            if (checkForOutstandingCommitments)
              CommitmentsPruningBound.Outstanding(noOutstandingCommitmentsF)
            else CommitmentsPruningBound.LastComputedAndSent(lastComputedAndSentF),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
        )
      }

      for {
        res1 <- safeToPrune(true)
        res2 <- safeToPrune(false)
        sortedReconciliationIntervals <- sortedReconciliationIntervalsProvider
          .reconciliationIntervals(now)
        tick = sortedReconciliationIntervals.tickBeforeOrAt(now).value
      } yield {
        res1 shouldBe Some(CantonTimestampSecond.MinValue)
        res2 shouldBe Some(tick)
      }
    }
  }

  val parallelism = PositiveNumeric.tryCreate(2)

  "AcsCommitmentProcessor" must {
    "compute commitments as expected" in {
      val contractSetup = Map(
        (coid(0, 0), (Set(alice, bob), ts(2), ts(4))),
        (coid(1, 0), (Set(alice, bob), ts(2), ts(5))),
        (coid(2, 0), (Set(alice, bob, carol), ts(7), ts(8))),
        (coid(3, 0), (Set(alice, bob, carol), ts(9), ts(9))),
      )

      val crypto = cryptoSetup(localId, topology)

      val stakeholderLookup = { (cid: LfContractId) =>
        contractSetup.get(cid).map(_._1).getOrElse(throw new Exception(s"unknown contract ID $cid"))
      }

      val acsF = acsSetup(contractSetup.fmap { case (_, createdAt, archivedAt) =>
        (createdAt.forgetRefinement, archivedAt.forgetRefinement)
      })

      def commitments(
          acs: ActiveContractSnapshot,
          at: CantonTimestampSecond,
      ): Future[Map[ParticipantId, AcsCommitment.CommitmentType]] =
        for {
          snapshotOrErr <- acs.snapshot(at.forgetRefinement)
          snapshot <- snapshotOrErr.fold(
            _ => Future.failed(new RuntimeException(s"Failed to get snapshot at timestamp $at")),
            sn => Future.successful(sn.map { case (cid, _ts) => cid -> stakeholderLookup(cid) }),
          )
          byStkhSet = snapshot.groupBy(_._2).map { case (stkhs, m) =>
            val h = LtHash16()
            m.keySet.foreach(cid => h.add(cid.encodeDeterministically.toByteArray))
            SortedSet(stkhs.toList: _*) -> h.getByteString()
          }
          res <- AcsCommitmentProcessor.commitments(
            localId,
            byStkhSet,
            crypto,
            at,
            None,
            parallelism,
          )
        } yield res

      for {
        acs <- acsF
        commitments1 <- commitments(acs, ts(1))
        commitments3 <- commitments(acs, ts(3))
        commitments4 <- commitments(acs, ts(4))
        commitments5 <- commitments(acs, ts(5))
        commitments7 <- commitments(acs, ts(7))
        commitments9 <- commitments(acs, ts(9))
        commitments10 <- commitments(acs, ts(10))
      } yield {
        assert(commitments1.isEmpty)
        assert(commitments3.contains(remoteId1))
        assert(!commitments3.contains(remoteId2))
        assert(!commitments3.contains(remoteId3))

        assert(commitments4.contains(remoteId1))
        assert(commitments4.get(remoteId1) != commitments3.get(remoteId1))

        assert(commitments5.isEmpty)

        assert(commitments7.contains(remoteId1))
        assert(commitments7.contains(remoteId2))
        assert(!commitments7.contains(remoteId3))
        assert(commitments7.get(remoteId1) == commitments7.get(remoteId2))
        assert(commitments7.get(remoteId1) != commitments4.get(remoteId1))
        assert(commitments7.get(remoteId1) != commitments5.get(remoteId1))

        assert(commitments9.isEmpty)
        assert(commitments10.isEmpty)

      }
    }

    // Covers the case where a previously hosted stakeholder got disabled
    "ignore contracts in the ACS snapshot where the participant doesn't host a stakeholder" in {
      val snapshot1 = Map(
        SortedSet(bob, carol) -> LtHash16().getByteString()
      )
      val snapshot2 = Map(
        SortedSet(bob, carol) -> LtHash16().getByteString(),
        SortedSet(alice, bob) -> LtHash16().getByteString(),
      )
      val snapshot3 = Map(
        SortedSet(alice, bob) -> LtHash16().getByteString()
      )
      val crypto = cryptoSetup(localId, topology)

      for {
        res1 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot1,
          crypto,
          ts(0),
          None,
          parallelism,
        )
        res2 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot2,
          crypto,
          ts(0),
          None,
          parallelism,
        )
        res3 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot3,
          crypto,
          ts(0),
          None,
          parallelism,
        )
      } yield {
        res1 shouldBe Map.empty
        res2.keySet shouldBe Set(remoteId1)
        res2 shouldEqual res3
      }
    }

    "correctly issue local and process buffered remote commitments" in {
      val timeProofs = List(3L, 6, 10, 16).map(CantonTimestamp.ofEpochSecond)
      val contractSetup = Map(
        // contract ID to stakeholders, creation and archival time
        (coid(0, 0), (Set(alice, bob), toc(1), toc(9))),
        (coid(0, 1), (Set(alice, carol), toc(9), toc(12))),
        (coid(1, 0), (Set(alice, carol), toc(1), toc(3))),
      )

      val topology = Map(
        localId -> Set(alice),
        remoteId1 -> Set(bob),
        remoteId2 -> Set(carol),
      )

      val (processor, store, _, changes) = testSetupDontPublish(timeProofs, contractSetup, topology)

      val remoteCommitments = List(
        (remoteId1, List(coid(0, 0)), ts(0), ts(5)),
        (remoteId2, List(coid(0, 1)), ts(5), ts(10)),
      )

      for {
        remote <- remoteCommitments.parTraverse(commitmentMsg)
        delivered = remote.map(cmt =>
          (
            cmt.message.period.toInclusive.plusSeconds(1),
            List(OpenEnvelope(cmt, Recipients.cc(localId), testedProtocolVersion)),
          )
        )
        // First ask for the remote commitments to be processed, and then compute locally
        _ <- delivered
          .parTraverse_ { case (ts, batch) =>
            processor.processBatchInternal(ts.forgetRefinement, batch)
          }
          .onShutdown(fail())
        _ = changes.foreach { case (ts, tb, change) =>
          processor.publish(RecordTime(ts, tb.v), change)
        }
        _ <- processor.queue.flush()
        computed <- store.searchComputedBetween(CantonTimestamp.Epoch, timeProofs.lastOption.value)
        received <- store.searchReceivedBetween(CantonTimestamp.Epoch, timeProofs.lastOption.value)
      } yield {
        verify(processor.sequencerClient, times(2)).sendAsync(
          any[Batch[DefaultOpenEnvelope]],
          any[SendType],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[SendCallback],
        )(anyTraceContext)
        assert(computed.size === 2)
        assert(received.size === 2)
      }
    }

    /*
     This test is disabled for protocol versions for which the reconciliation interval is
     static because the described setting cannot occur.
     */
    if (testedProtocolVersion >= ProtocolVersion.v4) {
      "work when commitment tick falls between two participants connection to the domain" in {
        /*
        The goal here is to check that ACS commitment processing works even when
        a commitment tick falls between two participants' connection timepoints to the domain.
        The reason this scenario is important is because the reconciliation interval (and
        thus ticks) is defined only from the connection time.

        We test the following scenario (timestamps are considered as seconds since epoch):
        - Reconciliation interval = 5s
        - Remote participant (RP) connects to the domain at t=0
        - Local participant (LP) connects to the domain at t=6
        - A shared contract lives between t=8 and t=12
        - RP sends a commitment with period (5, 10]
          Note: t=5 is not on a tick for LP
        - LP sends a commitment with period (0, 10]

        At t=13, we check that:
        - Nothing is outstanding at LP
        - Computed and received commitments are correct
         */

        interval shouldBe PositiveSeconds.ofSeconds(5)

        val timeProofs = List[Long](9, 13).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (coid(0, 0), (Set(alice, bob), toc(8), toc(12)))
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val sortedReconciliationIntervalsProvider = constantSortedReconciliationIntervalsProvider(
          interval,
          domainBootstrappingTime = CantonTimestamp.ofEpochSecond(6),
        )

        val (processor, store, _) = testSetup(
          timeProofs,
          contractSetup,
          topology,
          overrideDefaultSortedReconciliationIntervalsProvider =
            Some(sortedReconciliationIntervalsProvider),
        )

        val remoteCommitments = List((remoteId1, List(coid(0, 0)), ts(5), ts(10)))

        for {
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId), testedProtocolVersion)),
            )
          )
          // First ask for the remote commitments to be processed, and then compute locally
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())

          _ <- processor.queue.flush()

          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          outstanding <- store.outstanding(
            CantonTimestamp.MinValue,
            timeProofs.lastOption.value,
            None,
          )
        } yield {
          computed.size shouldBe 1
          inside(computed.headOption.value) { case (commitmentPeriod, participantId, _) =>
            commitmentPeriod shouldBe CommitmentPeriod
              .create(CantonTimestampSecond.MinValue, ts(10))
              .value
            participantId shouldBe remoteId1
          }

          received.size shouldBe 1

          inside(received.headOption.value) {
            case SignedProtocolMessage(
                  AcsCommitment(_, sender, counterParticipant, period, _),
                  _,
                ) =>
              sender shouldBe remoteId1
              counterParticipant shouldBe localId
              period shouldBe CommitmentPeriod.create(ts(5), ts(10)).value
          }

          outstanding shouldBe empty
        }
      }
    }

    "prevent pruning when there is no timestamp such that no commitments are outstanding" in {
      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(Future.successful(None))
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(0), CantonTimestamp.Epoch, CantonTimestamp.Epoch, None)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), CantonTimestamp.Epoch)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(0), CantonTimestamp.Epoch)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        res shouldEqual None
      }
    }

    "prevent pruning when there is no clean head in the request journal" in {
      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(Some(ts.min(CantonTimestamp.Epoch)))
        }
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        res shouldEqual Some(CantonTimestampSecond.MinValue)
      }
    }

    def assertInIntervalBefore(
        before: CantonTimestamp,
        reconciliationInterval: PositiveSeconds,
    ): Option[CantonTimestampSecond] => Assertion = {
      case None => fail()
      case Some(ts) =>
        val delta = JDuration.between(ts.toInstant, before.toInstant)
        delta should be > JDuration.ofSeconds(0)
        delta should be <= reconciliationInterval.unwrap
    }

    "prevent pruning of requests needed for crash recovery" in {
      val reconciliationInterval = PositiveSeconds.ofSeconds(1)
      val requestTsDelta = 20.seconds

      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
      val ts0 = CantonTimestamp.Epoch
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val ts2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val ts3 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      val ts4 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 5)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(0), ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts0)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(1), ts1, ts3.plusMillis(1))
        ) // RC1 commits after RC3
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(1), ts1)
        )
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(2), ts2, ts2))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(2), ts2)
        )
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(3), RequestState.Pending, ts3, None)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(3), ts3)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(RequestCounter(2), ts2))
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(4), RequestState.Pending, ts4, None)
        ) // Replay starts at ts4
        _ <- requestJournalStore
          .replace(RequestCounter(3), ts3, RequestState.Pending, RequestState.Clean, Some(ts3))
          .valueOrFail("advance RC 3 to clean")
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(4), ts4)
        )
        res2 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        withClue("request 1:") {
          assertInIntervalBefore(ts1, reconciliationInterval)(res1)
        } // Do not prune request 1
        // Do not prune request 1 as crash recovery may delete the dirty request 4 and then we're back in the same situation as for res1
        withClue("request 3:") { assertInIntervalBefore(ts1, reconciliationInterval)(res2) }
      }
    }

    "prevent pruning of the last request known to be clean" in {
      val reconciliationInterval = PositiveSeconds.ofSeconds(1)
      val requestTsDelta = 20.seconds

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val ts0 = CantonTimestamp.Epoch
      val tsCleanRequest = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 1)
      val ts3 = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 3)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(0), ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts0)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(2), tsCleanRequest)
        )
        _ <- requestJournalStore.insert(RequestData(RequestCounter(3), RequestState.Pending, ts3))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(4), ts3)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(2), tsCleanRequest)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield assertInIntervalBefore(tsCleanRequest, reconciliationInterval)(res)
    }

    "prevent pruning of dirty sequencer counters" in {
      val reconciliationInterval = PositiveSeconds.ofSeconds(1)
      val requestTsDelta = 20.seconds

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      // Clean sequencer counter is behind clean request counter
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val tsCleanRequest = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(2), tsCleanRequest)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts1)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        assertInIntervalBefore(ts1, reconciliationInterval)(res)
      }
    }

    "prevent pruning of events corresponding to in-flight requests" in {
      val reconciliationInterval = PositiveSeconds.ofSeconds(1)
      val requestTsDelta = 20.seconds

      val changeId1 = mkChangeIdHash(1)
      val changeId2 = mkChangeIdHash(2)

      val submissionId = LedgerSubmissionId.assertFromString("submission-id").some

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      // In-flight submission 2 and 3 are clean
      val tsCleanRequest = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val tsCleanRequest2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      // In-flight submission 1 has timed out
      val ts1 = CantonTimestamp.ofEpochSecond(-100)
      val submission1 = InFlightSubmission(
        changeId1,
        submissionId,
        domainId,
        new UUID(0, 1),
        UnsequencedSubmission(ts1, TestSubmissionTrackingData.default),
        traceContext,
      )
      val submission2 = InFlightSubmission(
        changeId2,
        submissionId,
        domainId,
        new UUID(0, 2),
        UnsequencedSubmission(CantonTimestamp.MaxValue, TestSubmissionTrackingData.default),
        traceContext,
      )
      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(3), tsCleanRequest2, tsCleanRequest2)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(3), tsCleanRequest2)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(1), tsCleanRequest2)
        )
        () <- inFlightSubmissionStore
          .register(submission1)
          .valueOrFailShutdown("register message ID 1")
        () <- inFlightSubmissionStore
          .register(submission2)
          .valueOrFailShutdown("register message ID 2")
        () <- inFlightSubmissionStore.observeSequencing(
          submission2.submissionDomain,
          Map(submission2.messageId -> SequencedSubmission(SequencerCounter(2), tsCleanRequest)),
        )
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        // Now remove the timed-out submission 1 and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission1.referenceByMessageId))
        res2 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        // Now remove the clean request and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission2.referenceByMessageId))
        res3 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        assertInIntervalBefore(submission1.associatedTimestamp, reconciliationInterval)(res1)
        assertInIntervalBefore(tsCleanRequest, reconciliationInterval)(res2)
        assertInIntervalBefore(tsCleanRequest2, reconciliationInterval)(res3)
      }
    }

    "running commitments work as expected" in {
      val rc =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)

      rc.watermark shouldBe RecordTime.MinValue
      rc.snapshot() shouldBe CommitmentSnapshot(
        RecordTime.MinValue,
        Map.empty,
        Map.empty,
        Set.empty,
      )
      val ch1 = AcsChange(
        activations = Map(
          coid(0, 0) -> withTestHash(ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)),
          coid(0, 1) -> withTestHash(ContractMetadata.tryCreate(Set.empty, Set(bob, carol), None)),
        ),
        deactivations = Map.empty,
      )
      rc.update(rt(1, 0), ch1)
      rc.watermark shouldBe rt(1, 0)
      val snap1 = rc.snapshot()
      snap1.rt shouldBe rt(1, 0)
      snap1.active.keySet shouldBe Set(SortedSet(alice, bob), SortedSet(bob, carol))
      snap1.delta.keySet shouldBe Set(SortedSet(alice, bob), SortedSet(bob, carol))
      snap1.deleted shouldBe Set.empty

      val ch2 = AcsChange(
        deactivations = Map(coid(0, 0) -> withTestHash(Set(alice, bob))),
        activations = Map(
          coid(1, 1) -> withTestHash(ContractMetadata.tryCreate(Set.empty, Set(alice, carol), None))
        ),
      )
      rc.update(rt(1, 1), ch2)
      rc.watermark shouldBe rt(1, 1)
      val snap2 = rc.snapshot()
      snap2.rt shouldBe rt(1, 1)
      snap2.active.keySet shouldBe Set(SortedSet(alice, carol), SortedSet(bob, carol))
      snap2.delta.keySet shouldBe Set(SortedSet(alice, carol))
      snap2.deleted shouldBe Set(SortedSet(alice, bob))

      val ch3 = AcsChange(
        deactivations = Map.empty,
        activations = Map(
          coid(2, 1) -> withTestHash(ContractMetadata.tryCreate(Set.empty, Set(alice, carol), None))
        ),
      )
      rc.update(rt(3, 0), ch3)
      val snap3 = rc.snapshot()
      snap3.rt shouldBe (rt(3, 0))
      snap3.active.keySet shouldBe Set(SortedSet(alice, carol), SortedSet(bob, carol))
      snap3.delta.keySet shouldBe Set(SortedSet(alice, carol))
      snap3.deleted shouldBe Set.empty
    }

    "contracts differing by contract hash only result in different commitments" in {
      val rc1 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val rc2 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val hash1 = ExampleTransactionFactory.lfHash(1)
      val hash2 = ExampleTransactionFactory.lfHash(2)
      hash1 should not be hash2

      // add a fixed contract id with custom hash and return active and delta-added commitments (byte strings)
      def addCommonContractId(
          rc: RunningCommitments,
          hash: LfHash,
      ): (AcsCommitment.CommitmentType, AcsCommitment.CommitmentType) = {
        val commonContractId = coid(0, 0)
        rc.watermark shouldBe RecordTime.MinValue
        rc.snapshot() shouldBe CommitmentSnapshot(
          RecordTime.MinValue,
          Map.empty,
          Map.empty,
          Set.empty,
        )
        val ch1 = AcsChange(
          activations = Map(
            commonContractId -> WithContractHash(
              ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
              hash,
            )
          ),
          deactivations = Map.empty,
        )
        rc.update(rt(1, 0), ch1)
        rc.watermark shouldBe rt(1, 0)
        val snapshot = rc.snapshot()
        snapshot.rt shouldBe rt(1, 0)
        snapshot.active.keySet shouldBe Set(SortedSet(alice, bob))
        snapshot.delta.keySet shouldBe Set(SortedSet(alice, bob))
        snapshot.deleted shouldBe Set.empty
        (snapshot.active(SortedSet(alice, bob)), snapshot.delta(SortedSet(alice, bob)))
      }

      val (activeCommitment1, deltaAddedCommitment1) = addCommonContractId(rc1, hash1)
      val (activeCommitment2, deltaAddedCommitment2) = addCommonContractId(rc2, hash2)
      activeCommitment1 should not be activeCommitment2
      deltaAddedCommitment1 should not be deltaAddedCommitment2
    }
  }
}

class AcsCommitmentProcessorSyncTest
    extends AnyWordSpec
    with AcsCommitmentProcessorBaseTest
    with HasExecutionContext
    with RepeatableTestSuiteTest {

  "retry on DB exceptions" in {
    val timeProofs = List(0L, 1).map(CantonTimestamp.ofEpochSecond)
    val contractSetup = Map(
      (coid(0, 0), (Set(alice, bob), toc(1), toc(9)))
    )

    val topology = Map(
      localId -> Set(alice),
      remoteId1 -> Set(bob),
    )

    val badStore = new ThrowOnWriteCommitmentStore()
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val (processor, _, _sequencerClient) = testSetup(
          timeProofs,
          contractSetup,
          topology,
          optCommitmentStore = Some(badStore),
        )
        eventually(timeUntilSuccess = FiniteDuration(30, TimeUnit.SECONDS)) {
          badStore.writeCounter.get() should be > 100
        }
        logger.info("Close the processor to stop retrying")
        processor.close()
      },
      forAll(_) {
        _.warningMessage should (include(
          s"Disconnect and reconnect to the domain ${domainId.toString} if this error persists."
        ) or include regex "Timeout .* expired, but tasks still running. Shutting down forcibly")
      },
    )
  }
}
