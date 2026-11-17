// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  GenericTopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{FloatingUpdate, SynchronizerUpdate, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{SuppressionRule, TracedLogger}
import com.digitalasset.canton.participant.admin.party.OnboardingClearanceScheduler
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.{ParticipantHistograms, ParticipantMetrics}
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.PendingLsuOperation
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryPendingOperationStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasActorSystem,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
}
import com.digitalasset.nonempty.NonEmpty
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

sealed abstract class ParticipantTopologyTerminateProcessingTest
    extends AsyncWordSpec
    with BaseTest
    with FailOnShutdown
    with HasExecutionContext
    with HasActorSystem
    with ProtocolVersionChecksAsyncWordSpec {

  protected def mkStore: TopologyStore[TopologyStoreId.SynchronizerStore]

  protected lazy val psid1 = DefaultTestIdentities.physicalSynchronizerId

  // use a new psid for each test case to isolate DB tests
  private val serialCounter = new AtomicInteger(psid1.serial.tryIncrement.value)
  protected def newPsid = PhysicalSynchronizerId(
    psid1.logical,
    NonNegativeInt.tryCreate(serialCounter.incrementAndGet()),
    psid1.protocolVersion,
  )
  private def synchronizerPredecessor(upgradeTime: CantonTimestamp) =
    SynchronizerPredecessor(psid1, upgradeTime, isLateUpgrade = false)

  private def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore] = mkStore,
      initialRecordTime: CantonTimestamp = CantonTimestamp.MinValue,
      synchronizerPredecessor: Option[SynchronizerPredecessor] = None,
      onboardingClearanceScheduler: OnboardingClearanceScheduler =
        mock[OnboardingClearanceScheduler],
      ledgerApiIndexer: LedgerApiIndexer = mock[LedgerApiIndexer],
  ): (
      ParticipantTopologyTerminateProcessing,
      TopologyStore[TopologyStoreId.SynchronizerStore],
      ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]],
      RecordOrderPublisher,
      PendingOnboardingClearanceStore,
  ) = {

    val eventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
      ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])

    val recordOrderPublisher = mock[RecordOrderPublisher]
    when(
      recordOrderPublisher.scheduleFloatingEventPublication(
        any[CantonTimestamp],
        eventCaptor.capture(),
        any[() => FutureUnlessShutdown[Unit]],
        any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
      )(any[TraceContext])
    )
      .thenReturn(Outcome(Right(FutureUnlessShutdown.unit)))

    val pendingOnboardingClearanceStoreMock = spy(
      new InMemoryPendingOperationStore[OnboardingClearanceOperation, SynchronizerId](
        OnboardingClearanceOperation,
        loggerFactory,
      )
    )

    val metrics = new ParticipantMetrics(
      new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
      new NoOpMetricsFactory,
    )

    val proc = new ParticipantTopologyTerminateProcessing(
      recordOrderPublisher,
      ledgerApiIndexer,
      store,
      initialRecordTime = initialRecordTime,
      DefaultTestIdentities.participant1,
      pauseSynchronizerIndexingDuringPartyReplication = false,
      synchronizerPredecessor = synchronizerPredecessor,
      pendingLsuOperationsStore = mock[PendingLsuOperation.Store],
      synchronizerConnectionConfigStore = mock[SynchronizerConnectionConfigStore],
      pendingOnboardingClearanceStore = pendingOnboardingClearanceStoreMock,
      onboardingClearanceScheduler = onboardingClearanceScheduler,
      partyReplicationTriggersO = None,
      metrics,
      loggerFactory,
    )
    (proc, store, eventCaptor, recordOrderPublisher, pendingOnboardingClearanceStoreMock)
  }

  val factory = new TestingOwnerWithKeys(
    DefaultTestIdentities.participant1,
    loggerFactory,
    parallelExecutionContext,
  )

  private def mkFlagCloseable(): FlagCloseable = new FlagCloseable {
    override protected def timeouts: ProcessingTimeout =
      ParticipantTopologyTerminateProcessingTest.this.timeouts
    override protected[this] def logger: TracedLogger =
      ParticipantTopologyTerminateProcessingTest.this.logger
  }

  val dummyFlagCloseable: FlagCloseable = mkFlagCloseable()

  import DefaultTestIdentities.*
  import factory.*

  private val trustCertEffective = CantonTimestamp.Epoch.plusSeconds(1000)
  private val noTraceContext: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]] =
    _ => FutureUnlessShutdown.pure(None)

  // populates the store with `numParties` party onboardings followed by the trust certificate, so that
  // the streamed onboarding recovery has `numParties + 1` topology updates to enqueue
  private def populate(
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      numParties: Int,
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- MonadUtil.sequentialTraverse_((1 to numParties).toList) { i =>
        val eff = CantonTimestamp.Epoch.plusSeconds(i.toLong)
        add(
          store,
          eff.minusSeconds(1),
          eff,
          List(partyParticipant1(PartyId.tryCreate(s"p$i", namespace))),
        )
      }
      _ <- add(
        store,
        trustCertEffective.minusSeconds(1),
        trustCertEffective,
        List(trustCertificate),
      )
    } yield ()

  private lazy val party1participant1 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Confirmation)),
    )
  )

  private lazy val party1participant1_ob = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Submission, onboarding = true)),
    )
  )

  private lazy val party1_p1_onboarding_p2_active = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Submission, onboarding = true),
        HostingParticipant(participant2, Submission, onboarding = false),
      ),
    )
  )

  private lazy val party1participant1_added = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Submission, onboarding = false)),
    )
  )

  private lazy val party1participant1_2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Confirmation),
        HostingParticipant(participant2, Submission),
      ),
    )
  )

  private lazy val party1participant2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant2, Submission)
      ),
    )
  )

  private lazy val party1participant1_2_threshold2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.two,
      Seq(
        HostingParticipant(participant1, Confirmation),
        HostingParticipant(participant2, Submission),
      ),
    )
  )

  private lazy val party1participant1_2_withChange = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Observation),
        HostingParticipant(participant2, Submission),
      ),
    )
  )

  private def partyParticipant1(
      party: PartyId
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = mkAdd(
    PartyToParticipant.tryCreate(
      party,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Confirmation)),
    )
  )

  private val trustCertificate: SignedTopologyTransaction[Replace, SynchronizerTrustCertificate] =
    mkAdd(
      SynchronizerTrustCertificate(
        DefaultTestIdentities.participant1,
        DefaultTestIdentities.synchronizerId,
      )
    )

  lazy val synchronizerParams1 = mkAdd(
    SynchronizerParametersState(
      synchronizerId,
      DynamicSynchronizerParameters
        .defaultValues(testedProtocolVersion)
        .update(reconciliationInterval = PositiveSeconds.tryOfSeconds(1)),
    )
  )
  lazy val synchronizerParams2 = mkAdd(
    SynchronizerParametersState(
      synchronizerId,
      DynamicSynchronizerParameters
        .defaultValues(testedProtocolVersion)
        .update(reconciliationInterval = PositiveSeconds.tryOfSeconds(2)),
    )
  )

  def add(
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): FutureUnlessShutdown[Unit] =
    add(store, timestamp, timestamp, transactions)

  def add(
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      sequencedTimestamp: CantonTimestamp,
      effectiveTimestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- store.update(
        SequencedTime(sequencedTimestamp),
        EffectiveTime(effectiveTimestamp),
        removals = transactions
          .map(tx => tx.mapping.uniqueKey -> (Some(tx.serial), Set.empty[TxHash]))
          .toMap,
        additions = transactions.map(ValidatedTopologyTransaction(_)),
      )
    } yield ()

  private def timestampWithCounter(idx: Int): (CantonTimestamp, SequencerCounter) =
    CantonTimestamp.Epoch.plusSeconds(idx.toLong) -> SequencerCounter(idx)

  val terminateProcessingSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[ParticipantTopologyTerminateProcessing] && SuppressionRule
      .LevelAndAbove(Level.WARN)

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  ParticipantTopologyTerminateProcessing.getClass.getSimpleName should {
    "notify of the party rights grant to the first participant" in {
      val (proc, store, eventCaptor, rop, _) = mk()
      val (cts, sc) = timestampWithCounter(0)

      val txs = List(party1participant1)

      for {
        _ <- add(store, cts, txs)
        _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts))
      } yield {
        verify(rop, times(1)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
          any[() => FutureUnlessShutdown[Unit]],
          any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
        )(any[TraceContext])
        val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
        events.size shouldBe 1
        forAll(events) {
          case TopologyTransactionEffective(_, events, _, _, _, _) =>
            forAll(events) {
              case PartyToParticipantAuthorization(
                    party,
                    participant,
                    Added(AuthorizationLevel.Confirmation),
                  ) =>
                party shouldBe party1.toLf
                participant shouldBe participant1.uid.toProtoPrimitive
              case PartyToParticipantAuthorization(_, _, _) =>
                fail("unexpected topology event")
            }
          case _ => fail("unexpected transaction type")
        }
      }
    }

    "notify of the party rights grant to the second participant" in {
      val (proc, store, eventCaptor, rop, _) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1))
        _ <- add(store, cts1, List(party1participant1_2))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        verify(rop, times(1)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
          any[() => FutureUnlessShutdown[Unit]],
          any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
        )(any[TraceContext])
        val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
        events.size shouldBe 1
        forAll(events) {
          case TopologyTransactionEffective(_, events, _, _, _, _) =>
            forAll(events) {
              case PartyToParticipantAuthorization(
                    party,
                    participant,
                    Added(AuthorizationLevel.Submission),
                  ) =>
                party shouldBe party1.toLf
                participant shouldBe participant2.uid.toProtoPrimitive
              case PartyToParticipantAuthorization(_, _, _) =>
                fail("unexpected topology event")
            }
          case _ => fail("unexpected transaction type")
        }
      }
    }

    "notify of the party rights changed for the first participant" in {
      val (proc, store, eventCaptor, rop, _) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1_2))
        _ <- add(store, cts1, List(party1participant1_2_withChange))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        verify(rop, times(1)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
          any[() => FutureUnlessShutdown[Unit]],
          any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
        )(any[TraceContext])
        val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
        events.size shouldBe 1
        forAll(events) {
          case TopologyTransactionEffective(_, events, _, _, _, _) =>
            forAll(events) {
              case PartyToParticipantAuthorization(
                    party,
                    participant,
                    ChangedTo(AuthorizationLevel.Observation),
                  ) =>
                party shouldBe party1.toLf
                participant shouldBe participant1.uid.toProtoPrimitive
              case PartyToParticipantAuthorization(_, _, _) =>
                fail("unexpected topology event")
            }
          case _ => fail("unexpected transaction type")
        }
      }
    }

    "terminate processing should take upgrade time into account" in {
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      def test(
          synchronizerPredecessor: Option[SynchronizerPredecessor],
          expectedEventsCount: Int,
      ) = {
        val (proc, store, eventCaptor, rop, _) =
          mk(synchronizerPredecessor = synchronizerPredecessor)

        for {
          _ <- add(store, cts0, List(party1participant1))
          _ <- add(store, cts1, List(party1participant1_2))
          _ <- proc.terminate(
            sc1,
            SequencedTime(cts1),
            EffectiveTime(cts1),
          )
        } yield {
          verify(rop, times(expectedEventsCount)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
            any[() => FutureUnlessShutdown[Unit]],
            any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
          events.size shouldBe expectedEventsCount
        }
      }

      for {
        _ <- test(None, expectedEventsCount = 1)
        _ <- test(Some(synchronizerPredecessor(cts1)), expectedEventsCount = 0)
        // event is before the upgrade time
        _ <- test(Some(synchronizerPredecessor(cts1.immediateSuccessor)), expectedEventsCount = 0)
      } yield succeed
    }

    "notify of the party rights revocation from the second participant" in {
      val (proc, store, eventCaptor, rop, _) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1_2))
        _ <- add(store, cts1, List(party1participant1))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        verify(rop, times(1)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
          any[() => FutureUnlessShutdown[Unit]],
          any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
        )(any[TraceContext])
        val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
        events.size shouldBe 1
        forAll(events) {
          case TopologyTransactionEffective(_, events, _, _, _, _) =>
            forAll(events) {
              case PartyToParticipantAuthorization(
                    party,
                    participant,
                    Revoked,
                  ) =>
                party shouldBe party1.toLf
                participant shouldBe participant2.uid.toProtoPrimitive
              case PartyToParticipantAuthorization(_, _, _) =>
                fail("unexpected topology event")
            }
          case _ => fail("unexpected transaction type")
        }
      }
    }

    "no events if the party rights threshold change" in {
      val (proc, store, eventCaptor, _rop, _) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1_2))
        _ <- add(store, cts1, List(party1participant1_2_threshold2))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        val events = eventCaptor.getAllValues.asScala
        events.size shouldBe 0
      }
    }

    "no events if no change" in {
      val (proc, store, eventCaptor, _rop, _) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1_2))
        _ <- add(store, cts1, List.empty)
        _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
      } yield {
        val events = eventCaptor.getAllValues.asScala
        events.size shouldBe 0
      }
    }

    "no event publication if below initialRecordTime" in {
      val (proc, store, _, rop, _) = mk(initialRecordTime = CantonTimestamp.ofEpochSecond(11))
      val (cts, sc) = timestampWithCounter(10)
      val (cts1, sc1) = timestampWithCounter(11)
      val (cts2, sc2) = timestampWithCounter(12)

      val txs = List(party1participant1)

      for {
        _ <- add(store, cts, txs)
        _ <- add(store, cts1, txs)
        _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts))
        _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        _ = {
          verify(rop, times(0)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
        }
        _ <- proc.terminate(sc2, SequencedTime(cts2), EffectiveTime(cts2))
      } yield {
        verify(rop, times(0)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
        )(any[TraceContext])
        succeed
      }
    }

    "fail if scheduling an event is not possible due to record order is already passed it" in {
      loggerFactory.suppress(terminateProcessingSuppressionRule) {
        val (proc, store, _, rop, _) = mk()
        val (cts, sc) = timestampWithCounter(10)

        val txs = List(party1participant1)

        when(
          rop.scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
            any[() => FutureUnlessShutdown[Unit]],
            any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
          )(any[TraceContext])
        )
          .thenReturn(Outcome(Left(CantonTimestamp.ofEpochSecond(15))))

        for {
          _ <- add(store, cts, txs)
          err <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts)).failed
        } yield {
          verify(rop, times(1)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
            any[() => FutureUnlessShutdown[Unit]],
            any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
          )(any[TraceContext])
          err.getMessage should include(
            "Cannot schedule topology event as record time is already at"
          )
        }
      }
    }

    "re-schedule recovery events correctly" in {
      val (proc, store, eventCaptor, rop, _) =
        mk(initialRecordTime = CantonTimestamp.ofEpochSecond(20))
      val (cts, _) = timestampWithCounter(10)
      val (cts2, _) = timestampWithCounter(12)
      val (cts3, _) = timestampWithCounter(15)
      val (cts4, _) = timestampWithCounter(17)
      val (cts5, _) = timestampWithCounter(20)
      val (cts6, _) = timestampWithCounter(21)
      val (cts7, _) = timestampWithCounter(23)
      val (cts8, _) = timestampWithCounter(25)
      val (cts9, _) = timestampWithCounter(27)

      val parties = List(1, 2, 3, 4, 5, 6, 7, 8).map(i =>
        PartyId(UniqueIdentifier.tryCreate(s"p$i", namespace))
      )
      val traceContexts = List.fill(8)(TraceContext.createNew("test"))
      traceContexts.toSet.size shouldBe 8
      val traceContextMap = List(cts, cts2, cts3, cts4, cts5, cts6, cts7, cts8)
        .zip(traceContexts)
        .toMap
      def traceContextLookup: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]] =
        t => FutureUnlessShutdown.pure(traceContextMap.get(t))
      def extractFromUpdates(
          updates: List[Update]
      ): List[(Set[LfPartyId], CantonTimestamp, TraceContext)] =
        updates.collect { case u: TopologyTransactionEffective =>
          (
            u.events.collect { case ptp: PartyToParticipantAuthorization =>
              ptp.party
            },
            u.recordTime,
            u.traceContext,
          )
        }

      when(
        rop.scheduleFloatingEventPublication(
          any[CantonTimestamp],
          eventCaptor.capture(),
        )(any[TraceContext])
      ).thenAnswer { (timestamp: CantonTimestamp, _: CantonTimestamp => Option[Update]) =>
        Outcome(
          if (timestamp == CantonTimestamp.ofEpochSecond(20))
            Left(CantonTimestamp.ofEpochSecond(20))
          else
            Right(())
        )
      }

      val immediateEventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
        ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])
      when(
        rop.scheduleFloatingEventPublicationImmediately(
          immediateEventCaptor.capture()
        )(any[TraceContext])
      )
        .thenReturn(Outcome(CantonTimestamp.ofEpochSecond(20)))
      for {
        _ <- add(store, cts.minusSeconds(2), cts.minusSeconds(1), List(trustCertificate))
        _ <- add(store, cts, cts2, List(partyParticipant1(parties.head))) // before
        _ <- add(store, cts2, cts4, List(partyParticipant1(parties(1)))) // before
        _ <- add(
          store,
          cts3,
          cts5,
          List(partyParticipant1(parties(2))),
        ) // on (conditional schedule)
        _ <- add(store, cts4, cts6, List(partyParticipant1(parties(3)))) // schedule
        _ <- add(
          store,
          cts5,
          cts7,
          List(
            partyParticipant1(parties(4)),
            partyParticipant1(parties(5)),
          ),
        ) // schedule
        _ <- add(
          store,
          cts6,
          cts8,
          List(partyParticipant1(parties(6))),
        ) // future processing, no schedule
        _ <- add(
          store,
          cts7,
          cts9,
          List(partyParticipant1(parties(7))),
        ) // future processing, no schedule
        _ <- proc.scheduleMissingTopologyEventsAtInitialization(
          topologyEventPublishedOnInitialRecordTime = true,
          traceContextLookup,
          PositiveInt.two,
          dummyFlagCloseable,
        )
        _ = {
          verify(rop, times(3)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          verify(rop, times(0)).scheduleFloatingEventPublicationImmediately(
            any[CantonTimestamp => Option[FloatingUpdate]]
          )(any[TraceContext])
          extractFromUpdates(
            eventCaptor.getAllValues.asScala.toList
              .flatMap(Option(_)) // due to the two matchers, there could be nulls here
              .flatMap(_(CantonTimestamp.MinValue))
          ).toSet shouldBe Set(
            (Set(parties(2).toLf), cts5, traceContextMap(cts3)),
            (Set(parties(3).toLf), cts6, traceContextMap(cts4)),
            (Set(parties(4).toLf, parties(5).toLf), cts7, traceContextMap(cts5)),
          )
        }
        _ <- proc.scheduleMissingTopologyEventsAtInitialization(
          topologyEventPublishedOnInitialRecordTime = false,
          traceContextLookup,
          PositiveInt.two,
          dummyFlagCloseable,
        )
      } yield {
        verify(rop, times(6)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
        )(any[TraceContext])
        verify(rop, times(1)).scheduleFloatingEventPublicationImmediately(
          any[CantonTimestamp => Option[FloatingUpdate]]
        )(any[TraceContext])
        extractFromUpdates(
          immediateEventCaptor.getAllValues.asScala.toList
            .flatMap(_(CantonTimestamp.ofEpochSecond(20)))
        ).toSet shouldBe Set(
          (Set(parties(2).toLf), cts5, traceContextMap(cts3))
        )
      }
    }

    "resume streamed onboarding recovery after a crash without losing or repeating events" in {
      val effectiveTimes = (1 to 5).map(i => CantonTimestamp.Epoch.plusSeconds(i.toLong))
      val parties = (1 to 5).map(i => PartyId.tryCreate(s"p$i", namespace))
      val trustCertEffective = CantonTimestamp.Epoch.plusSeconds(10)
      val postOnboardingParty = PartyId.tryCreate("post-stc", namespace)

      val store = mkStore
      val noTraceContext: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]] =
        _ => FutureUnlessShutdown.pure(None)

      // runs recovery for initial topology processing (like after a restart) with the given persisted initial record time and
      // returns the updates enqueued into the ledger api indexer by that run
      def recoverFrom(
          initialRecordTime: CantonTimestamp,
          topologyEventPublishedOnInitialRecordTime: Boolean = false,
      ): FutureUnlessShutdown[
        (Seq[(CantonTimestamp, Set[LfPartyId])], Seq[(CantonTimestamp, Set[LfPartyId])])
      ] = {
        val enqueued = new java.util.concurrent.ConcurrentLinkedQueue[Update]()
        val indexer = mock[LedgerApiIndexer]
        when(indexer.enqueue).thenReturn { (update: Update) =>
          enqueued.add(update)
          FutureUnlessShutdown.unit
        }
        val (proc, _, _, rop, _) = mk(store, initialRecordTime, ledgerApiIndexer = indexer)
        // capture the event by the protocol processing crash recovery
        val timestampCaptor: ArgumentCaptor[CantonTimestamp] =
          ArgumentCaptor.forClass(classOf[CantonTimestamp])
        val eventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
          ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])
        when(
          rop.scheduleFloatingEventPublication(
            timestampCaptor.capture(),
            eventCaptor.capture(),
          )(anyTraceContext)
        )
          .thenReturn(Outcome(Right(())))

        proc
          .scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime = topologyEventPublishedOnInitialRecordTime,
            noTraceContext,
            PositiveInt.two,
            dummyFlagCloseable,
          )
          .map { _ =>
            // extract the scheduled event
            val scheduled = eventCaptor.getValue.apply(timestampCaptor.getValue).toList
            (summarize(enqueued.asScala.toList), summarize(scheduled))
          }
      }

      def summarize(updates: Seq[Update]): Seq[(CantonTimestamp, Set[LfPartyId])] =
        updates.collect { case u: TopologyTransactionEffective =>
          u.recordTime -> u.events.collect { case ptp: PartyToParticipantAuthorization =>
            ptp.party
          }
        }

      for {
        _ <- MonadUtil.sequentialTraverse_(effectiveTimes.zip(parties)) { case (eff, party) =>
          add(store, eff.minusSeconds(1), eff, List(partyParticipant1(party)))
        }
        _ <- add(
          store,
          trustCertEffective.minusSeconds(1),
          trustCertEffective,
          List(trustCertificate),
        )
        // add another party with an effective time after the trust certificate
        _ <- add(
          store,
          trustCertEffective.minusMillis(500),
          trustCertEffective.plusMillis(500),
          List(partyParticipant1(postOnboardingParty)),
        )
        // first run: nothing persisted yet
        (full, fullScheduled) <- recoverFrom(CantonTimestamp.MinValue)
        // crash after the first two updates were persisted: the restart starts from the persisted record time
        (resumed, resumedScheduled) <- recoverFrom(effectiveTimes(1))
        // recover from a timestamp that falls between two effective times
        (resumedInBetween, resumedScheduledInBetween) <- recoverFrom(
          effectiveTimes(1).immediateSuccessor
        )
        // crash after everything was persisted, but before the recovery completed
        (afterSTC, afterScheduled) <- recoverFrom(trustCertEffective)
        (afterSTC, afterScheduled) <- recoverFrom(trustCertEffective)

      } yield {
        val allPartiesInInitialTopologyState = effectiveTimes.zip(
          parties.map(p => Set(p.toLf))
        ) :+ (trustCertEffective -> Set(trustCertificate.mapping.participantId.adminParty.toLf))
        val scheduledParty =
          Seq(trustCertEffective.plusMillis(500) -> Set(postOnboardingParty.toLf))

        full shouldBe allPartiesInInitialTopologyState
        fullScheduled shouldBe scheduledParty

        resumed shouldBe allPartiesInInitialTopologyState.drop(2)
        resumedScheduled shouldBe scheduledParty

        resumedInBetween shouldBe resumed
        resumedScheduledInBetween shouldBe resumedScheduled

        // persisted prefix and resumed suffix together are exactly the full stream
        (full.take(2) ++ resumed) shouldBe full

        afterSTC shouldBe empty
        afterScheduled shouldBe scheduledParty
      }
    }

    // Recovery with an initial record time after the trust certificate's effective time: the indexer
    // already holds the whole onboarding state, so the streamed onboarding recovery is skipped. The
    // protocol processing recovery still has to schedule the events of transactions that are already in
    // the store (from the onboarding snapshot or from processing) but only become effective after the
    // initial record time.
    //
    //  sequenced:  2   9   11  12  14  16  18
    //  effective:  4   10  13  15  17  19  21
    //              pre STC A   B   C   D   E
    // pre and STC are always skipped
    case class RecoveryAfterOnboarding(
        enqueued: Seq[Update],
        scheduledEffectiveTimes: Seq[CantonTimestamp],
        scheduledParties: Seq[Set[LfPartyId]],
        immediateParties: Seq[Set[LfPartyId]],
    )

    def party(name: String) = PartyId.tryCreate(name, namespace)
    def recoverAfterOnboarding(
        initialRecordTime: CantonTimestamp,
        topologyEventPublishedOnInitialRecordTime: Boolean,
    ): FutureUnlessShutdown[RecoveryAfterOnboarding] = {
      def at(sec: Long) = CantonTimestamp.Epoch.plusSeconds(sec)
      def partiesOf(updates: Seq[Update]): Seq[Set[LfPartyId]] =
        updates.collect { case u: TopologyTransactionEffective =>
          u.events.collect { case ptp: PartyToParticipantAuthorization => ptp.party }
        }

      val store = mkStore
      val enqueued = new java.util.concurrent.ConcurrentLinkedQueue[Update]()
      val indexer = mock[LedgerApiIndexer]
      when(indexer.enqueue).thenReturn { (update: Update) =>
        enqueued.add(update)
        FutureUnlessShutdown.unit
      }
      val (proc, _, _, rop, _) = mk(store, initialRecordTime, ledgerApiIndexer = indexer)

      val timestampCaptor: ArgumentCaptor[CantonTimestamp] =
        ArgumentCaptor.forClass(classOf[CantonTimestamp])
      val eventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
        ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])
      // the record time is already at the initial record time: scheduling at it is rejected
      when(
        rop.scheduleFloatingEventPublication(timestampCaptor.capture(), eventCaptor.capture())(
          anyTraceContext
        )
      ).thenAnswer { (timestamp: CantonTimestamp, _: CantonTimestamp => Option[Update]) =>
        Outcome(if (timestamp == initialRecordTime) Left(initialRecordTime) else Right(()))
      }
      val immediateCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
        ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])
      when(
        rop.scheduleFloatingEventPublicationImmediately(immediateCaptor.capture())(anyTraceContext)
      )
        .thenReturn(Outcome(initialRecordTime))

      for {
        _ <- add(store, at(1), at(3), List(partyParticipant1(party("pre"))))
        _ <- add(store, at(9), at(10), List(trustCertificate))
        _ <- add(store, at(11), at(13), List(partyParticipant1(party("a"))))
        _ <- add(store, at(12), at(15), List(partyParticipant1(party("b"))))
        _ <- add(store, at(14), at(17), List(partyParticipant1(party("c"))))
        _ <- add(store, at(16), at(19), List(partyParticipant1(party("d"))))
        _ <- add(store, at(18), at(21), List(partyParticipant1(party("e"))))
        _ <- proc.scheduleMissingTopologyEventsAtInitialization(
          topologyEventPublishedOnInitialRecordTime = topologyEventPublishedOnInitialRecordTime,
          _ => FutureUnlessShutdown.pure(Some(traceContext)),
          PositiveInt.two,
          dummyFlagCloseable,
        )
      } yield {
        val scheduledEffectiveTimes = timestampCaptor.getAllValues.asScala.toList
        val scheduledUpdates =
          scheduledEffectiveTimes.zip(eventCaptor.getAllValues.asScala).flatMap {
            case (t, factory) => factory(t)
          }
        RecoveryAfterOnboarding(
          enqueued = enqueued.asScala.toList,
          scheduledEffectiveTimes = scheduledEffectiveTimes,
          scheduledParties = partiesOf(scheduledUpdates).map(_.map(identity)),
          immediateParties = partiesOf(
            immediateCaptor.getAllValues.asScala.toList.flatMap(_(initialRecordTime))
          ),
        )
      }
    }

    "recovery after the trust certificate from a record time between effective times" in {
      val c = party("c").toLf
      val d = party("d").toLf
      // 16 is between the effective times 15 (b) and 17 (c)
      recoverAfterOnboarding(
        CantonTimestamp.Epoch.plusSeconds(16),
        topologyEventPublishedOnInitialRecordTime = false,
      ).map { result =>
        // nothing is streamed: the onboarding recovery is skipped
        result.enqueued shouldBe empty
        // c and d are sequenced before and effective after the initial record time, e is reprocessed
        result.scheduledEffectiveTimes should contain theSameElementsAs Seq(
          CantonTimestamp.Epoch.plusSeconds(17),
          CantonTimestamp.Epoch.plusSeconds(19),
        )
        result.scheduledParties should contain theSameElementsAs Seq(Set(c), Set(d))
        result.immediateParties shouldBe empty
      }
    }

    "recovery after the trust certificate from an effective time, topology event already published" in {
      val c = party("c").toLf
      val d = party("d").toLf
      recoverAfterOnboarding(
        CantonTimestamp.Epoch.plusSeconds(17), // effective time of c
        topologyEventPublishedOnInitialRecordTime = true,
      ).map { result =>
        result.enqueued shouldBe empty
        result.scheduledEffectiveTimes should contain theSameElementsAs Seq(
          CantonTimestamp.Epoch.plusSeconds(17),
          CantonTimestamp.Epoch.plusSeconds(19),
        )
        result.scheduledParties should contain theSameElementsAs Seq(Set(c), Set(d))
        // c is omitted: its event was already published at the initial record time
        result.immediateParties shouldBe empty
        // c is omitted: its event was already published at the initial record time
        result.immediateParties shouldBe empty
      }
    }

    "recovery after the trust certificate from an effective time, topology event not yet published" in {
      val c = party("c").toLf
      val d = party("d").toLf
      recoverAfterOnboarding(
        CantonTimestamp.Epoch.plusSeconds(17), // effective time of c
        topologyEventPublishedOnInitialRecordTime = false,
      ).map { result =>
        result.enqueued shouldBe empty
        result.scheduledEffectiveTimes should contain theSameElementsAs Seq(
          CantonTimestamp.Epoch.plusSeconds(17),
          CantonTimestamp.Epoch.plusSeconds(19),
        )
        result.scheduledParties should contain theSameElementsAs Seq(Set(c), Set(d))
        // c is published immediately at the initial record time
        result.immediateParties shouldBe Seq(Set(c))
      }
    }

    "onboarding recovery: stop streaming and report a shutdown when the flagCloseable closes mid-stream" in {
      val numParties = 40
      val store = mkStore
      val flagCloseable = mkFlagCloseable()

      val enqueuedCount = new AtomicInteger(0)
      val firstEnqueueStarted = Promise[Unit]()
      val releaseEnqueue = Promise[Unit]()
      val indexer = mock[LedgerApiIndexer]
      when(indexer.enqueue).thenReturn { (_: Update) =>
        // block the very first enqueue, so that the stream is guaranteed to be in the middle of processing
        if (enqueuedCount.getAndIncrement() == 0) {
          firstEnqueueStarted.success(())
          FutureUnlessShutdown.outcomeF(releaseEnqueue.future)
        } else FutureUnlessShutdown.unit
      }
      val (proc, _, _, rop, _) = mk(store, ledgerApiIndexer = indexer)

      for {
        _ <- populate(store, numParties)
        result = proc
          .scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime = false,
            noTraceContext,
            PositiveInt.two,
            flagCloseable,
          )
          .unwrap
        _ <- FutureUnlessShutdown.outcomeF(firstEnqueueStarted.future)
        // closing runs the registered kill switch; it must not wait for the blocked enqueue
        _ = flagCloseable.close()
        _ = releaseEnqueue.success(())
        outcome <- FutureUnlessShutdown.outcomeF(result)
      } yield {
        outcome shouldBe AbortedDueToShutdown
        // the kill switch stops reading from the store, so only what was already buffered in the stream is enqueued
        enqueuedCount.get() should (be >= 1 and be < numParties + 1)
        // the protocol processing recovery must not run after the onboarding recovery was aborted
        verify(rop, times(0)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
        )(any[TraceContext])
        succeed
      }
    }

    "onboarding recovery: report a shutdown if the flagCloseable is already closed before the recovery starts" in {
      val store = mkStore
      val flagCloseable = mkFlagCloseable()
      val indexer = mock[LedgerApiIndexer]
      when(indexer.enqueue).thenReturn((_: Update) => FutureUnlessShutdown.unit)
      val (proc, _, _, rop, _) = mk(store, ledgerApiIndexer = indexer)

      for {
        _ <- populate(store, 5)
        _ = flagCloseable.close()
        outcome <- FutureUnlessShutdown.outcomeF(
          proc
            .scheduleMissingTopologyEventsAtInitialization(
              topologyEventPublishedOnInitialRecordTime = false,
              noTraceContext,
              PositiveInt.two,
              flagCloseable,
            )
            .unwrap
        )
      } yield {
        outcome shouldBe AbortedDueToShutdown
        verify(rop, times(0)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
        )(any[TraceContext])
        succeed
      }
    }

    "onboarding recovery: complete normally if the flagCloseable only closes after the recovery finished" in {
      val numParties = 5
      val store = mkStore
      val flagCloseable = mkFlagCloseable()
      val enqueuedCount = new AtomicInteger(0)
      val indexer = mock[LedgerApiIndexer]
      when(indexer.enqueue).thenReturn { (_: Update) =>
        enqueuedCount.incrementAndGet()
        FutureUnlessShutdown.unit
      }
      val (proc, _, _, _, _) = mk(store, ledgerApiIndexer = indexer)

      for {
        _ <- populate(store, numParties)
        _ <- proc.scheduleMissingTopologyEventsAtInitialization(
          topologyEventPublishedOnInitialRecordTime = false,
          noTraceContext,
          PositiveInt.two,
          flagCloseable,
        )
      } yield {
        // the close registration of the finished recovery must be gone, so that closing is harmless
        flagCloseable.close()
        // every party and the trust certificate resulted in an update
        enqueuedCount.get() shouldBe numParties + 1
      }
    }

    "query pending operations when local party is onboarded" in {
      val (proc, store, _, _, pendingOnboardingStore) = mk()
      val (cts0, _) = timestampWithCounter(0)

      for {
        _ <- add(store, cts0, List(party1participant1_ob))
        _ <- proc.terminate(
          SequencerCounter(0),
          SequencedTime(cts0),
          EffectiveTime(cts0),
        )
      } yield {

        verify(pendingOnboardingStore, times(1)).get(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])

        succeed
      }
    }

    "request the onboarding flag clearance at the effective time of the onboarding" in {
      val scheduler = mock[OnboardingClearanceScheduler]
      val (proc, store, _, _, pendingOnboardingStore) = mk(onboardingClearanceScheduler = scheduler)
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)
      // A pending clearance with another effective time than the onboarding's
      val pendingClearance = OnboardingClearanceOperation(Some(EffectiveTime(cts0)))(
        OnboardingClearanceOperation.protocolVersionRepresentativeFor(testedProtocolVersion)
      ).toPendingOperation(store.storeId.psid.logical, party1)

      for {
        _ <- valueOrFail(pendingOnboardingStore.insert(pendingClearance))("insert")
        _ <- add(store, cts1, List(party1participant1_ob))
        _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
      } yield {
        verify(scheduler, times(1))
          .requestClearanceInBackground(eqTo(party1), eqTo(EffectiveTime(cts1)))(any[TraceContext])
        succeed
      }
    }

    "clear pending operations when onboarding is cleared for a local party" in {
      val (proc, store, _, _, pendingOnboardingStore) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        _ <- add(store, cts0, List(party1participant1_ob))
        _ <- add(store, cts1, List(party1participant1_added))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {

        verify(pendingOnboardingStore, times(1)).delete(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])

        succeed
      }
    }

    "not interact with clearance stores for a non-local party" in {
      val (proc, store, _, _, pendingOnboardingStore) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      val party1participant2_ob = mkAdd(
        PartyToParticipant.tryCreate(
          party1,
          PositiveInt.one,
          Seq(HostingParticipant(participant2, Submission, onboarding = true)),
        )
      )
      val party1participant2_added = mkAdd(
        PartyToParticipant.tryCreate(
          party1,
          PositiveInt.one,
          Seq(HostingParticipant(participant2, Submission, onboarding = false)),
        )
      )

      for {
        _ <- add(store, cts0, List(party1participant2_ob))
        _ <- add(store, cts1, List(party1participant2_added))
        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        // Assert that because the participant is non-local, the guard `if (eventInfo.onboardingLocallyHostedParty || eventInfo.clearingOnboardingLocallyHostedParty)` prevents execution
        verify(pendingOnboardingStore, never).get(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])
        verify(pendingOnboardingStore, never).delete(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])
        succeed
      }
    }

    "clear pending operations when a local party's onboarding is revoked while remaining hosted elsewhere" in {
      val (proc, store, _, _, pendingOnboardingStore) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        // Initial State: Party is on both participants P1 (local, onboarding) and P2 (remote, active)
        _ <- add(store, cts0, List(party1_p1_onboarding_p2_active))

        // (Implicitly) revoke the onboarding party on P1 by replacing the mapping with P2 only
        _ <- add(store, cts1, List(party1participant2))

        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {

        verify(pendingOnboardingStore, times(1)).delete(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])

        succeed
      }
    }

    "not interact with clearance stores when a remote party is revoked while local remains onboarding" in {
      val (proc, store, _, _, pendingOnboardingStore) = mk()
      val (cts0, _) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)

      for {
        // Initial State: Party is on both participants P1 (local, onboarding) and P2 (remote, active)
        _ <- add(store, cts0, List(party1_p1_onboarding_p2_active))

        // Revoke party on P2 while still onboarding the party P1
        _ <- add(store, cts1, List(party1participant1_ob))

        _ <- proc.terminate(
          sc1,
          SequencedTime(cts1),
          EffectiveTime(cts1),
        )
      } yield {
        // Because the party is still onboarding on the local participant (P1), we do not touch the local onboaridng clearance store
        verify(pendingOnboardingStore, never).delete(
          any[SynchronizerId],
          any[String],
          any[NonEmptyString],
        )(any[TraceContext])
        succeed
      }
    }

    "only query the effective state changes for the relevant topology mappings for the target protocol version" in {
      val (cts1, sc1) = timestampWithCounter(1)

      val mockStore = mock[TopologyStore[SynchronizerStore]]
      val mappingCodeCaptor: ArgumentCaptor[Option[NonEmpty[Seq[TopologyMapping.Code]]]] =
        ArgumentCaptor.forClass(classOf[Option[NonEmpty[Seq[TopologyMapping.Code]]]])
      when(
        mockStore.findEffectiveStateChanges(
          any[CantonTimestamp],
          mappingCodeCaptor.capture(),
          any[Boolean],
        )(anyTraceContext)
      ).thenReturn(FutureUnlessShutdown.pure(Seq.empty))
      when(mockStore.storeId).thenReturn(SynchronizerStore(synchronizerId.toPhysical))
      when(mockStore.protocolVersion).thenReturn(synchronizerId.toPhysical.protocolVersion)

      val (proc, _, _, _, _) = mk(store = mockStore)
      for {
        _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
      } yield {
        mappingCodeCaptor
          .getValue()
          .value
          .forgetNE should contain theSameElementsAs ParticipantTopologyTerminateProcessing
          .relevantMappingsForEffectiveStateChanges(testedProtocolVersion)
      }
    }

    "notify of dynamic synchronizer parameter changes" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      val (proc, store, eventCaptor, rop, _) = mk()
      val (cts0, sc0) = timestampWithCounter(0)
      val (cts1, sc1) = timestampWithCounter(1)
      val (cts2, sc2) = timestampWithCounter(2)

      for {
        _ <- add(store, cts0, List(synchronizerParams1))
        _ <- proc.terminate(sc0, SequencedTime(cts0), EffectiveTime(cts0))
        _ <- add(store, cts1, List(synchronizerParams2))
        _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        // store the same parameter transaction again, e.g. because of an additional signature
        _ <- add(store, cts2, List(synchronizerParams2))
        _ <- proc.terminate(sc2, SequencedTime(cts2), EffectiveTime(cts2))
      } yield {
        // the most recently stored topology transaction doesn't trigger a floating publication, because no actual change was stored
        verify(rop, times(2)).scheduleFloatingEventPublication(
          any[CantonTimestamp],
          any[CantonTimestamp => Option[FloatingUpdate]],
          any[() => FutureUnlessShutdown[Unit]],
          any[(SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown[Unit]],
        )(any[TraceContext])
        val events =
          eventCaptor.getAllValues.asScala.zip(Seq(cts0, cts1)).flatMap { case (f, ts) => f(ts) }
        events.size shouldBe 2

        forAll(events.zip(List(synchronizerParams1, synchronizerParams2))) {
          case (TopologyTransactionEffective(_, _, genericTopologyEvents, _, _, _), expected) =>
            genericTopologyEvents.loneElement shouldBe GenericTopologyEvent.SynchronizerParametersState
              .fromTopologyTransaction(expected.transaction)
          case _ => fail("unexpected transaction type")
        }
      }
    }
  }
}

final class ParticipantTopologyTerminateProcessingTestInMemory
    extends ParticipantTopologyTerminateProcessingTest {
  override protected def mkStore: TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(newPsid),
      predecessor = None,
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )
}

sealed abstract class DbParticipantTopologyTerminateProcessingTest
    extends ParticipantTopologyTerminateProcessingTest
    with DbTest
    with DbTopologyStoreHelper {
  override protected def mkStore: TopologyStore[SynchronizerStore] = {
    val psid = newPsid
    mkStore(psid, psid.toString)
  }
}

final class ParticipantTopologyTerminateProcessingTestPostgres
    extends DbParticipantTopologyTerminateProcessingTest
    with PostgresTest

final class ParticipantTopologyTerminateProcessingTestH2
    extends DbParticipantTopologyTerminateProcessingTest
    with H2Test
