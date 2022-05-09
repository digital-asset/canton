// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  SignedTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission._
import com.digitalasset.canton.topology.transaction.TrustLevel._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, BaseTestWordSpec, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class BaseDomainTopologyClientTest extends BaseTestWordSpec {

  private class TestClient() extends BaseDomainTopologyClient {

    override def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override def trySnapshot(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): TopologySnapshotLoader =
      ???
    override def domainId: DomainId = ???

    def advance(ts: CantonTimestamp): Unit = {
      this.observed(SequencedTime(ts), EffectiveTime(ts), 0, List())
    }
    override implicit def executionContext: ExecutionContext = ???
    override protected def loggerFactory: NamedLoggerFactory =
      BaseDomainTopologyClientTest.this.loggerFactory
    override protected def clock: Clock = ???
    override def currentSnapshotApproximation(implicit
        traceContext: TraceContext
    ): TopologySnapshotLoader = ???
    override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = ???
  }

  "waiting for snapshots" should {

    val ts1 = CantonTimestamp.Epoch
    val ts2 = ts1.plusSeconds(60)

    "announce snapshot if there is one" in {
      val tc = new TestClient()
      tc.advance(ts1)
      tc.snapshotAvailable(ts1) shouldBe true
      tc.snapshotAvailable(ts2) shouldBe false
      tc.advance(ts2)
      tc.snapshotAvailable(ts2) shouldBe true
    }

    "correctly get notified" in {
      val tc = new TestClient()
      val wt = tc.awaitTimestamp(ts2, true)
      wt match {
        case Some(fut) =>
          tc.advance(ts1)
          fut.isCompleted shouldBe false
          tc.advance(ts2)
          fut.isCompleted shouldBe true
        case None => fail("expected future")
      }
    }

    "just return a none if snapshot already exists" in {
      val tc = new TestClient()
      tc.advance(ts1)
      val wt = tc.awaitTimestamp(ts1, waitForEffectiveTime = true)
      wt shouldBe None
    }

  }

}

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
trait StoreBasedTopologySnapshotTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import EffectiveTimeTestHelpers._

  def topologySnapshot(mk: () => TopologyStore): Unit = {

    val factory = new TestingOwnerWithKeys(
      DefaultTestIdentities.participant1,
      loggerFactory,
      parallelExecutionContext,
    )
    import DefaultTestIdentities._
    import factory.TestingTransactions._
    import factory._

    lazy val party2participant1 = mkAdd(
      PartyToParticipant(RequestSide.Both, party1, participant1, Confirmation)
    )
    lazy val party2participant2a = mkAdd(
      PartyToParticipant(RequestSide.From, party2, participant1, Submission)
    )
    lazy val party2participant2b = mkAdd(
      PartyToParticipant(RequestSide.To, party2, participant1, Submission)
    )
    lazy val party2participant3 = mkAdd(
      PartyToParticipant(RequestSide.Both, party2, participant2, Submission)
    )

    class Fixture(initialKeys: Map[KeyOwner, Seq[SigningPublicKey]] = Map()) {
      val store = mk()
      val client =
        new StoreBasedDomainTopologyClient(
          mock[Clock],
          domainId,
          store,
          initialKeys,
          StoreBasedDomainTopologyClient.NoPackageDependencies,
          DefaultProcessingTimeouts.testing,
          loggerFactory,
        )

      def add(
          timestamp: CantonTimestamp,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      ): Future[Unit] = {

        val (adds, removes, _) = SignedTopologyTransactions(transactions).split

        store
          .updateState(
            SequencedTime(timestamp),
            EffectiveTime(timestamp),
            removes.result.map(_.uniquePath),
            adds.result,
          )
          .map { _ =>
            client.observed(timestamp, timestamp, 1, transactions)
          }
      }

    }

    "work with empty store" in {
      val fixture = new Fixture()
      import fixture._
      val _ = client.currentSnapshotApproximation
      val mrt = client.approximateTimestamp
      val sp = client.trySnapshot(mrt)
      for {
        participants <- sp.participants()
        parties <- sp.activeParticipantsOf(party1.toLf)
        keys <- sp.signingKeys(participant1)
      } yield {
        participants shouldBe empty
        parties shouldBe empty
        keys shouldBe empty
      }
    }

    def compareMappings(
        result: Map[ParticipantId, ParticipantAttributes],
        expected: Map[ParticipantId, ParticipantPermission],
    ) =
      result.map(x => (x._1, x._2.permission)) shouldBe expected

    def compareKeys(result: Seq[SigningPublicKey], expected: Seq[SigningPublicKey]) =
      result.map(_.fingerprint) shouldBe expected.map(_.fingerprint)

    "deliver correct results" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.add(
          ts,
          Seq(
            ns1k2,
            okm1,
            party2participant1,
            party2participant2a,
            party2participant2b,
            ps1,
            party2participant3,
          ),
        )
        _ = fixture.client.observed(ts.immediateSuccessor, ts.immediateSuccessor, 0, Seq())
        recent = fixture.client.currentSnapshotApproximation
        party1Mappings <- recent.activeParticipantsOf(party1.toLf)
        party2Mappings <- recent.activeParticipantsOf(party2.toLf)
        keys <- recent.signingKeys(domainManager)
      } yield {
        party1Mappings.keySet shouldBe Set(participant1)
        party1Mappings.get(participant1).map(_.permission) should contain(Confirmation)
        party2Mappings.keySet shouldBe Set(participant1)
        party2Mappings.get(participant1).map(_.permission) should contain(Submission)
        keys.map(_.id) shouldBe Seq(namespaceKey.id)
      }
    }

    "properly deals with participants with lower domain privileges" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.add(ts, Seq(ns1k2, okm1, party2participant1, ps2))
        _ = fixture.client.observed(ts.immediateSuccessor, ts.immediateSuccessor, 0, Seq())
        snapshot <- fixture.client.snapshot(ts.immediateSuccessor)
        party1Mappings <- snapshot.activeParticipantsOf(party1.toLf)
      } yield {
        compareMappings(party1Mappings, Map(participant1 -> Observation))
      }
    }

    "work properly with updates" in {
      val fixture = new Fixture()
      val ts2 = ts1.plusSeconds(1)
      for {
        _ <- fixture.add(
          ts,
          Seq(
            ns1k2,
            okm1,
            party2participant1,
            party2participant2a,
            party2participant2b,
            ps1,
            party2participant3,
          ),
        )
        _ <- fixture.add(ts1, Seq(rokm1, okm2, rps1, ps2, ps3))
        _ <- fixture.add(
          ts2,
          Seq(factory.revert(ps2), factory.mkAdd(ps1m.copy(permission = Disabled))),
        )
        _ = fixture.client.observed(ts2.immediateSuccessor, ts2.immediateSuccessor, 0, Seq())
        snapshotA <- fixture.client.snapshot(ts1)
        snapshotB <- fixture.client.snapshot(ts1.immediateSuccessor)
        snapshotC <- fixture.client.snapshot(ts2.immediateSuccessor)
        party1Ma <- snapshotA.activeParticipantsOf(party1.toLf)
        party1Mb <- snapshotB.activeParticipantsOf(party1.toLf)
        party2Ma <- snapshotA.activeParticipantsOf(party2.toLf)
        party2Mb <- snapshotB.activeParticipantsOf(party2.toLf)
        party2Mc <- snapshotC.activeParticipantsOf(party2.toLf)
        keysDMa <- snapshotA.signingKeys(domainManager)
        keysDMb <- snapshotB.signingKeys(domainManager)
        keysSa <- snapshotA.signingKeys(sequencer)
        keysSb <- snapshotB.signingKeys(sequencer)
        partPermA <- snapshotA.participantState(participant1)
        partPermB <- snapshotB.participantState(participant1)
        partPermC <- snapshotC.participantState(participant1)
        admin1a <- snapshotA.activeParticipantsOf(participant1.adminParty.toLf)
        admin1b <- snapshotB.activeParticipantsOf(participant1.adminParty.toLf)
      } yield {
        compareMappings(party1Ma, Map(participant1 -> Confirmation))
        compareMappings(party1Mb, Map(participant1 -> Observation))
        compareMappings(party2Ma, Map(participant1 -> Submission))
        compareMappings(party2Mb, Map(participant1 -> Observation, participant2 -> Confirmation))
        compareMappings(party2Mc, Map(participant2 -> Confirmation))
        compareKeys(keysDMa, Seq(namespaceKey))
        compareKeys(keysDMb, Seq())
        compareKeys(keysSa, Seq())
        compareKeys(keysSb, Seq(SigningKeys.key2))
        partPermA.permission shouldBe Submission
        partPermB.permission shouldBe Observation
        partPermC.permission shouldBe Disabled
        compareMappings(admin1a, Map(participant1 -> Submission))
        compareMappings(admin1b, Map(participant1 -> Observation))
      }
    }

    "mixin initialisation keys" in {
      val f = new Fixture(Map(sequencer -> Seq(SigningKeys.key6)))
      for {
        _ <- f.add(ts, Seq(ns1k2, okm1))
        _ <- f.add(ts1, Seq(okm2))
        _ = f.client.observed(ts1.immediateSuccessor, ts1.immediateSuccessor, 0, Seq())
        spA <- f.client.snapshot(ts1)
        spB <- f.client.snapshot(ts1.immediateSuccessor)
        dmKeys <- spA.signingKeys(domainManager)
        seqKeyA <- spA.signingKeys(sequencer)
        seqKeyB <- spB.signingKeys(sequencer)
      } yield {
        compareKeys(dmKeys, Seq(namespaceKey))
        compareKeys(seqKeyA, Seq(SigningKeys.key6))
        compareKeys(seqKeyB, Seq(SigningKeys.key2))
      }
    }

    "not show single sided party to participant mappings" in {
      val f = new Fixture()
      for {
        _ <- f.add(ts, Seq(ps1, party2participant2b))
        _ <- f.add(ts1, Seq(party2participant2a))
        _ = f.client.observed(ts1.immediateSuccessor, ts1.immediateSuccessor, 0, Seq())
        snapshot1 <- f.client.snapshot(ts1)
        snapshot2 <- f.client.snapshot(ts1.immediateSuccessor)
        res1 <- snapshot1.activeParticipantsOf(party2.toLf)
        res2 <- snapshot2.activeParticipantsOf(party2.toLf)
      } yield {
        res1 shouldBe empty
        compareMappings(res2, Map(participant1 -> Submission))
      }
    }

    "compute correct permissions for multiple mappings" in {
      val txs = Seq(
        PartyToParticipant(RequestSide.From, party1, participant1, Confirmation),
        PartyToParticipant(RequestSide.From, party1, participant1, Submission),
        PartyToParticipant(RequestSide.To, party1, participant1, Submission),
        ParticipantState(
          RequestSide.From,
          domainId,
          participant1,
          Observation,
          TrustLevel.Ordinary,
        ),
        ParticipantState(RequestSide.To, domainId, participant1, Submission, TrustLevel.Vip),
        PartyToParticipant(RequestSide.From, party2, participant2, Submission),
        PartyToParticipant(RequestSide.To, party2, participant2, Observation),
        ParticipantState(
          RequestSide.From,
          domainId,
          participant2,
          Confirmation,
          TrustLevel.Ordinary,
        ),
        ParticipantState(RequestSide.To, domainId, participant2, Confirmation, TrustLevel.Vip),
        PartyToParticipant(RequestSide.Both, party3, participant3, Submission),
        ParticipantState(RequestSide.Both, domainId, participant3, Confirmation, TrustLevel.Vip),
        ParticipantState(RequestSide.To, domainId, participant3, Submission, TrustLevel.Ordinary),
      )
      val f = new Fixture()
      def get(tp: TopologySnapshot, party: PartyId) = {
        tp.activeParticipantsOf(party.toLf).map { res =>
          res.map { case (p, r) =>
            (p, (r.permission, r.trustLevel))
          }
        }
      }
      val party4 = PartyId(UniqueIdentifier(Identifier.tryCreate(s"unrelated"), namespace))
      for {
        _ <- f.add(ts, txs.map(mkAdd(_)))
        _ = f.client.observed(ts1.immediateSuccessor, ts1.immediateSuccessor, 0, Seq())
        sp <- f.client.snapshot(ts1)
        p1 <- get(sp, party1)
        p2 <- get(sp, party2)
        p3 <- get(sp, party3)
        bulk <- sp.activeParticipantsOfParties(List(party1, party2, party3, party4).map(_.toLf))
      } yield {
        p1 shouldBe Map(participant1 -> ((Observation, Ordinary)))
        p2 shouldBe Map(participant2 -> ((Observation, Ordinary)))
        p3 shouldBe Map(participant3 -> ((Confirmation, Vip)))
        bulk shouldBe Map(
          party1.toLf -> Set(participant1),
          party2.toLf -> Set(participant2),
          party3.toLf -> Set(participant3),
        )
      }
    }

  }
}

class StoreBasedTopologySnapshotTestInMemory extends StoreBasedTopologySnapshotTest {
  "InMemoryTopologyStore" should {
    behave like topologySnapshot(() => new InMemoryTopologyStore(loggerFactory))
  }
}

trait DbStoreBasedTopologySnapshotTest extends StoreBasedTopologySnapshotTest {

  this: AsyncWordSpec with BaseTest with HasExecutionContext with DbTest =>

  protected def pureCryptoApi: CryptoPureApi

  "DbStoreBasedTopologySnapshot" should {
    behave like topologySnapshot(() =>
      new DbTopologyStore(
        storage,
        TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
        100,
        timeouts,
        loggerFactory,
      )
    )
  }
}
