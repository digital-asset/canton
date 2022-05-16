// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton._
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, TimeoutDuration}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology._
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
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  CommitmentSnapshot,
  CommitmentsPruningBound,
  RunningCommitments,
}
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentStore,
  InMemoryActiveContractStore,
  InMemoryInFlightSubmissionStore,
  InMemoryRequestJournalStore,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.participant.{RequestCounter, pruning}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  DefaultOpenEnvelope,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits._

@nowarn("msg=match may not be exhaustive")
trait AcsCommitmentProcessorBaseTest extends BaseTest {

  val interval = PositiveSeconds.ofSeconds(5)
  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))
  val localId = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain"))
  val remoteId1 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant1::domain")
  )
  val remoteId2 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )
  val remoteId3 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::domain")
  )

  val List(alice, bob, carol) =
    List("Alice::1", "Bob::2", "Carol::3").map(LfPartyId.assertFromString)

  val topology = Map(
    localId -> Set(alice),
    remoteId1 -> Set(bob),
    remoteId2 -> Set(carol),
  )

  protected val protocolVersion = TestDomainParameters.defaultStatic.protocolVersion

  def ts(i: Int): CantonTimestampSecond = CantonTimestampSecond.ofEpochSecond(i.longValue)

  def toc(timestamp: Int, requestCounter: Int = 0): TimeOfChange =
    TimeOfChange(requestCounter.toLong, ts(timestamp).forgetSecond)

  def mkChangeIdHash(index: Int) = ChangeIdHash(DefaultDamlValues.lfhash(index))

  def acsSetup(
      lifespan: Map[LfContractId, (CantonTimestamp, CantonTimestamp)]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[ActiveContractSnapshot] = {
    val acs = new InMemoryActiveContractStore(loggerFactory)
    lifespan.toList
      .traverse_ { case (cid, (createdTs, archivedTs)) =>
        for {
          _ <- acs.createContract(cid, TimeOfChange(0, createdTs)).value
          _ <- acs.archiveContract(cid, TimeOfChange(0, archivedTs)).value
        } yield ()
      }
      .map(_ => acs)
  }

  def cryptoSetup(
      owner: ParticipantId,
      topology: Map[ParticipantId, Set[LfPartyId]],
  ): SyncCryptoClient = {
    val topologyWithPermissions =
      topology.fmap(_.map(p => (p, ParticipantPermission.Submission)).toMap)
    TestingTopology().withReversedTopology(topologyWithPermissions).build().forOwnerAndDomain(owner)
  }

  def changesAtToc(
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
  def testSetupDontPublish(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[LfContractId, (Set[LfPartyId], TimeOfChange, TimeOfChange)],
      topology: Map[ParticipantId, Set[LfPartyId]],
      killSwitch: => Unit = (),
      optCommitmentStore: Option[AcsCommitmentStore] = None,
  )(implicit ec: ExecutionContext): (
      AcsCommitmentProcessor,
      AcsCommitmentStore,
      SequencerClient,
      List[(CantonTimestamp, Long, AcsChange)],
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
      (timeProofs.map(ts => TimeOfChange(0, ts)) ++ contractSetup.values.toList.flatMap {
        case (_, creationTs, archivalTs) =>
          List(creationTs, archivalTs)
      }).distinct.sorted
    val changes = changeTimes.map(changesAtToc(contractSetup))
    val store = optCommitmentStore.getOrElse(new InMemoryAcsCommitmentStore(loggerFactory))
    val acsCommitmentProcessor = new AcsCommitmentProcessor(
      domainId,
      localId,
      sequencerClient,
      domainCrypto,
      interval,
      store,
      (_, _) => FutureUnlessShutdown.unit,
      killSwitch,
      ParticipantTestMetrics.pruning,
      protocolVersion,
      DefaultProcessingTimeouts.testing
        .copy(storageMaxRetryInterval = TimeoutDuration.tryFromDuration(1.millisecond)),
      loggerFactory,
    )

    (acsCommitmentProcessor, store, sequencerClient, changes)
  }

  def testSetup(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[LfContractId, (Set[LfPartyId], TimeOfChange, TimeOfChange)],
      topology: Map[ParticipantId, Set[LfPartyId]],
      killSwitch: => Unit = (),
      optCommitmentStore: Option[AcsCommitmentStore] = None,
  )(implicit
      ec: ExecutionContext
  ): (AcsCommitmentProcessor, AcsCommitmentStore, SequencerClient) = {

    val (acsCommitmentProcessor, store, sequencerClient, changes) =
      testSetupDontPublish(timeProofs, contractSetup, topology, killSwitch, optCommitmentStore)

    changes.foreach { case (ts, rc, acsChange) =>
      acsCommitmentProcessor.publish(RecordTime(ts, rc), acsChange)
    }
    (acsCommitmentProcessor, store, sequencerClient)
  }

  val testHash = ExampleTransactionFactory.lfHash(0)

  def withTestHash[A] = WithContractHash[A](_, testHash)

  def rt(timestamp: Int, tieBreaker: Int) =
    RecordTime(ts(timestamp).forgetSecond, tieBreaker.toLong)

  val coid = (txId, discriminator) => ExampleTransactionFactory.suffixedId(txId, discriminator)
}

class AcsCommitmentProcessorTest extends AsyncWordSpec with AcsCommitmentProcessorBaseTest {

  import AcsCommitmentProcessorTestHelpers._

  def mkPeriod(interval: PositiveSeconds)(times: (Long, Long)): CommitmentPeriod =
    times match {
      case (after: Long, beforeAndAt: Long) =>
        CommitmentPeriod(
          CantonTimestamp.ofEpochSecond(after),
          CantonTimestamp.ofEpochSecond(beforeAndAt),
          interval,
        ).value
    }

  "AcsCommitmentProcessor.commitmentPeriodPreceding" must {
    "compute reasonable commitment periods for a basic example" in {
      val eventTimestamps = List(-2, 6, 24, 25, 26, 31L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods =
        List(
          (CantonTimestamp.MinValue.getEpochSecond, -5L),
          (-5L, 5L),
          (5L, 20L),
          (20L, 25L),
          (25L, 30L),
        )
          .map(mkPeriod(interval))

      timeProofPeriodFlow(interval, eventTimestamps) shouldBe expectedPeriods
    }

    "work for a commitment period that starts at MinValue and is smaller than the reconciliation interval" in {
      val longIntervalSeconds = 5 * scala.math.pow(10, 10L).toLong
      val longInterval = PositiveSeconds.ofSeconds(longIntervalSeconds)
      val eventTimestamps = List(-5L, 2L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods =
        List(
          (CantonTimestamp.MinValue.getEpochSecond, -longIntervalSeconds),
          (-longIntervalSeconds, 0L),
        )
          .map(mkPeriod(longInterval))
      timeProofPeriodFlow(longInterval, eventTimestamps) shouldBe expectedPeriods
    }

    "work when MinValue.getEpochSeconds isn't a multiple of the reconciliation interval" in {
      val longInterval = PositiveSeconds.ofDays(100)
      val longIntervalSeconds = longInterval.unwrap.getSeconds
      assert(
        CantonTimestamp.MinValue.getEpochSecond % longIntervalSeconds != 0,
        "Precondition for the test to make sense",
      )
      val eventTimestamps =
        List(2L, 5L, longIntervalSeconds + 2L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods =
        List((CantonTimestamp.MinValue.getEpochSecond, 0L), (0L, longIntervalSeconds))
          .map(mkPeriod(longInterval))
      timeProofPeriodFlow(longInterval, eventTimestamps) shouldBe expectedPeriods
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
          reconciliationInterval = longInterval,
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
          reconciliationInterval = longInterval,
        )
      } yield res shouldBe Some(CantonTimestampSecond.MinValue)
    }

    "take checkForOutstandingCommitments flag into account" in {
      val longInterval = PositiveSeconds.ofDays(100)
      val now = CantonTimestamp.now()

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
          reconciliationInterval = longInterval,
        )
      }

      for {
        res1 <- safeToPrune(true)
        res2 <- safeToPrune(false)
      } yield {
        res1 shouldBe Some(CantonTimestampSecond.MinValue)
        res2 shouldBe Some(AcsCommitmentProcessor.tickBeforeOrAt(now, longInterval))
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
        (createdAt.forgetSecond, archivedAt.forgetSecond)
      })

      def commitments(
          acs: ActiveContractSnapshot,
          at: CantonTimestampSecond,
      ): Future[Map[ParticipantId, AcsCommitment.CommitmentType]] =
        for {
          snapshotOrErr <- acs.snapshot(at.forgetSecond)
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

      val (processor, store, _sequencerClient, changes) =
        testSetupDontPublish(timeProofs, contractSetup, topology)

      // This is duplicating the internal logic of the commitment computation, but I don't have a better solution at the moment
      // if we want to test whether commitment buffering works
      // Also assumes that all the contract IDs in the list have the same stakeholders
      def commitment(cids: List[LfContractId]): AcsCommitment.CommitmentType = {
        val h = LtHash16()
        cids.foreach { cid =>
          h.add((testHash.bytes.toByteString concat cid.encodeDeterministically).toByteArray)
        }
        val doubleH = LtHash16()
        doubleH.add(h.get())
        doubleH.getByteString()
      }

      def commitmentMsg(
          params: (ParticipantId, List[LfContractId], CantonTimestampSecond, CantonTimestampSecond)
      ): Future[SignedProtocolMessage[AcsCommitment]] =
        params match {
          case (remote, cids, fromExclusive, toInclusive) =>
            val crypto =
              TestingTopology().withParticipants(remote).build().forOwnerAndDomain(remote)
            val cmt = commitment(cids)
            val snapshotF = crypto.snapshot(CantonTimestamp.Epoch)
            val period =
              CommitmentPeriod(fromExclusive.forgetSecond, toInclusive.forgetSecond, interval).value
            val payload = AcsCommitment.create(
              domainId,
              remote,
              localId,
              period,
              cmt,
              protocolVersion,
            )

            snapshotF.flatMap { snapshot =>
              SignedProtocolMessage.tryCreate(payload, snapshot, crypto.pureCrypto)
            }
        }

      val remoteCommitments = List(
        (remoteId1, List(coid(0, 0)), ts(0), ts(5)),
        (remoteId2, List(coid(0, 1)), ts(5), ts(10)),
      )

      for {
        remote <- remoteCommitments.traverse(commitmentMsg)
        delivered = remote.map(cmt =>
          (
            cmt.message.period.toInclusive.plusSeconds(1),
            List(OpenEnvelope(cmt, Recipients.cc(localId))),
          )
        )
        // First ask for the remote commitments to be processed, and then compute locally
        _ <- delivered
          .traverse_ { case (ts, batch) => processor.processBatchInternal(ts.forgetSecond, batch) }
          .onShutdown(fail())
        _ = changes.foreach { case (ts, tb, change) =>
          processor.publish(RecordTime(ts, tb), change)
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

    "prevent pruning when there is no timestamp such that no commitments are outstanding" in {
      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(Future.successful(None))
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(0L, CantonTimestamp.Epoch, CantonTimestamp.Epoch, None)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(0L, CantonTimestamp.Epoch)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(0L, CantonTimestamp.Epoch))
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          StaticDomainParameters.defaultReconciliationInterval,
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
          StaticDomainParameters.defaultReconciliationInterval,
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

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
      val ts0 = CantonTimestamp.Epoch
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val ts2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val ts3 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      val ts4 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 5)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(0L, ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(0L, ts0))
        _ <- requestJournalStore.insert(
          RequestData.clean(1L, ts1, ts3.plusMillis(1))
        ) // RC1 commits after RC3
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(1L, ts1))
        _ <- requestJournalStore.insert(RequestData.clean(2L, ts2, ts2))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(2L, ts2))
        _ <- requestJournalStore.insert(RequestData(3L, RequestState.Pending, ts3, None))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(3L, ts3))
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(2L, ts2))
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          reconciliationInterval,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        _ <- requestJournalStore.insert(
          RequestData(4L, RequestState.Pending, ts4, None)
        ) // Replay starts at ts4
        _ <- requestJournalStore
          .replace(3L, ts3, RequestState.Pending, RequestState.Clean, Some(ts3))
          .valueOrFail("advance RC 3 to clean")
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(4L, ts4))
        res2 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          reconciliationInterval,
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

      val ts0 = CantonTimestamp.Epoch
      val tsCleanRequest = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 1)
      val ts3 = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 3)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(0L, ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(0L, ts0))
        _ <- requestJournalStore.insert(RequestData.clean(2L, tsCleanRequest, tsCleanRequest))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(2L, tsCleanRequest)
        )
        _ <- requestJournalStore.insert(RequestData(3L, RequestState.Pending, ts3))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(4L, ts3))
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(2L, tsCleanRequest))
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          reconciliationInterval,
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
        _ <- requestJournalStore.insert(RequestData.clean(2L, tsCleanRequest, tsCleanRequest))
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(2L, tsCleanRequest))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(CursorPrehead(0L, ts1))
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          reconciliationInterval,
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
        _ <- requestJournalStore.insert(RequestData.clean(2L, tsCleanRequest, tsCleanRequest))
        _ <- requestJournalStore.insert(RequestData.clean(3L, tsCleanRequest2, tsCleanRequest2))
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(3L, tsCleanRequest2))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(1L, tsCleanRequest2)
        )
        () <- inFlightSubmissionStore
          .register(submission1)
          .valueOrFailShutdown("register message ID 1")
        () <- inFlightSubmissionStore
          .register(submission2)
          .valueOrFailShutdown("register message ID 2")
        () <- inFlightSubmissionStore.observeSequencing(
          submission2.submissionDomain,
          Map(submission2.messageId -> SequencedSubmission(2L, tsCleanRequest)),
        )
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          reconciliationInterval,
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
          reconciliationInterval,
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
          reconciliationInterval,
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

/* Scalacheck doesn't play nice with AsyncWordSpec, so using AnyWordSpec and waiting on futures */
class AcsCommitmentProcessorPropertyTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckDrivenPropertyChecks
    with HasExecutionContext {
  import AcsCommitmentProcessorTestHelpers._

  val hashOps: HashOps = new SymbolicPureCrypto
  "AcsCommitmentProcessor" must {

    val interval = PositiveSeconds.ofSeconds(5)

    "compute tick periods with no gaps" in {

      implicit lazy val incrementArb: Arbitrary[Seq[(Long, Long)]] =
        Arbitrary(for {
          len <- Gen.choose(0, 10)
          seq1 <- Gen.containerOfN[List, Long](len, Gen.choose(1L, 20L))
          seq2 <- Gen.containerOfN[List, Long](len, Gen.choose(0L, 999999L))
        } yield seq1.zip(seq2))

      implicit lazy val arbInstant: Arbitrary[CantonTimestamp] = Arbitrary(for {
        delta <- Gen.choose(0L, 20L)
      } yield CantonTimestamp.ofEpochSecond(delta))

      // Prevent scalacheck from shrinking, as it doesn't use the above generators and thus doesn't respect the required invariants
      import Shrink.shrinkAny

      forAll {
        (
            startTs: CantonTimestamp,
            increments: Seq[(Long, Long)],
        ) =>
          val incrementsToTimes =
            (increments: Seq[(Long, Long)]) =>
              increments.scanLeft(startTs) { case (ts, (diffS, diffMicros)) =>
                ts.plusSeconds(diffS).addMicros(diffMicros)
              }

          val times = incrementsToTimes(increments)
          val periods = timeProofPeriodFlow(interval, times)

          periods.foreach { period =>
            assert(
              JDuration.between(
                period.fromExclusive.toInstant,
                period.toInclusive.toInstant,
              ) >= interval.unwrap,
              "Commitment periods must be longer than the specified interval",
            )
            assert(period.toInclusive.microsOverSecond() == 0, "period must end at whole seconds")
            assert(
              period.fromExclusive.microsOverSecond() == 0,
              "period must start at whole seconds",
            )
            assert(
              period.toInclusive.getEpochSecond % interval.unwrap.getSeconds == 0,
              "period must end at commitment ticks",
            )
            assert(
              period.fromExclusive.getEpochSecond % interval.unwrap.getSeconds == 0,
              "period must start at commitment ticks",
            )
          }

          val gaps = periods.lazyZip(periods.drop(1)).map { case (p1, p2) =>
            JDuration.between(p1.toInclusive.toInstant, p2.fromExclusive.toInstant)
          }

          gaps.foreach { g =>
            assert(g === JDuration.ZERO, s"All gaps must be zero; times: $times; periods: $periods")
          }
      }

    }
  }
}

object AcsCommitmentProcessorTestHelpers {

  // Just empty commit sets (i.e., time proofs)
  def timeProofPeriodFlow(
      interval: PositiveSeconds,
      times: Seq[CantonTimestamp],
  ): Seq[CommitmentPeriod] = {
    times.foldLeft((None: Option[CantonTimestampSecond], Seq.empty[CommitmentPeriod])) {
      case ((lastTick, periods), ts) =>
        AcsCommitmentProcessor.commitmentPeriodPreceding(interval, lastTick)(ts) match {
          case Some(period) => (Some(period.toInclusive), periods :+ period)
          case None => (lastTick, periods)
        }
    } match {
      case (_lastTickNoLongerNeeded, commitmentPeriods) => commitmentPeriods
    }
  }

}
