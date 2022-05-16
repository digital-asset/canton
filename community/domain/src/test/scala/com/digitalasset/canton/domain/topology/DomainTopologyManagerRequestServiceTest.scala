// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.implicits._
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.domain.topology.RegisterTopologyTransactionRequestState._
import com.digitalasset.canton.domain.topology.RequestProcessingStrategy.{
  AutoApproveStrategy,
  AutoRejectStrategy,
  QueueStrategy,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.store.{TopologyStore, ValidatedTopologyTransaction}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class DomainTopologyManagerRequestServiceTest extends AsyncWordSpec with BaseTest {

  import DefaultTestIdentities._

  private implicit val iec: ExecutionContext = directExecutionContext
  private val clock = new WallClock(timeouts, loggerFactory)

  private val syncCrypto =
    TestingTopology()
      .withParticipants(participant1)
      .build(loggerFactory)
      .forOwnerAndDomain(participant1)
  private val p1Key =
    syncCrypto.ips.currentSnapshotApproximation.signingKey(participant1).futureValue.value
  private val p1KeyEnc =
    syncCrypto.ips.currentSnapshotApproximation.encryptionKey(participant1).futureValue.value

  private def genSigned[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      key: SigningPublicKey,
  ): SignedTopologyTransaction[Op] =
    FutureUtil
      .noisyAwaitResult(
        SignedTopologyTransaction
          .create(
            transaction,
            key,
            syncCrypto.pureCrypto,
            syncCrypto.crypto.privateCrypto,
            ProtocolVersion.latestForTest,
          )
          .value,
        "generate signed",
        10.seconds,
      )
      .value

  private def addToStore(
      store: TopologyStore,
      transactions: SignedTopologyTransaction[TopologyChangeOp]*
  ): Future[Unit] = {
    val ts = clock.now
    store.append(
      SequencedTime(ts),
      EffectiveTime(ts),
      transactions.map(ValidatedTopologyTransaction(_, None)).toList,
    )
  }

  private val p1Mapping =
    genSigned(TopologyStateUpdate.createAdd(OwnerToKeyMapping(participant1, p1Key)), p1Key)
  private val p1MappingEnc =
    genSigned(TopologyStateUpdate.createAdd(OwnerToKeyMapping(participant1, p1KeyEnc)), p1Key)
  private val partyMapping = genSigned(
    TopologyStateUpdate.createAdd(
      PartyToParticipant(RequestSide.Both, party1, participant1, ParticipantPermission.Submission)
    ),
    p1Key,
  )

  private def generateBase(
      autoEnable: Boolean,
      strategy: RequestProcessingStrategy,
      manager: DomainTopologyManager = mock[DomainTopologyManager],
  ) = {
    val store = new InMemoryTopologyStore(loggerFactory)
    val service =
      new DomainTopologyManagerRequestService(strategy, store, syncCrypto.pureCrypto, loggerFactory)
    if (!autoEnable)
      verify(manager, never).authorize(
        any[TopologyTransaction[TopologyChangeOp]],
        any[Option[Fingerprint]],
        anyBoolean,
        anyBoolean,
      )(anyTraceContext) // should not auto enable
    (manager, store, service)
  }

  "auto-approve strategy" should {

    def generate(autoEnable: Boolean = true) = {
      val manager = mock[DomainTopologyManager]
      val client = mock[DomainTopologyClient]
      when(manager.clock).thenReturn(clock)
      when(manager.managerId).thenReturn(domainManager)
      val strategy =
        new AutoApproveStrategy(
          manager,
          client,
          autoEnable,
          false,
          ProcessingTimeout(),
          loggerFactory,
        ) {
          override def awaitParticipantIsActive(pid: ParticipantId)(implicit
              traceContext: TraceContext
          ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure(true)
        }
      generateBase(autoEnable, strategy, manager)
    }

    "reject invalid signatures" in {
      val (manager, store, service) = generate()
      val p2SigKey = TestingIdentityFactory(loggerFactory).newSigningPublicKey(participant2)
      val faulty = p1Mapping.copy(key = p2SigKey)(ProtocolVersion.latestForTest, None)
      for {
        res <- service.newRequest(List(faulty))
      } yield {
        res should have length (1)
        res.map(_.state) shouldBe Seq(
          Failed(
            show"Signature check failed with SignatureWithWrongKey(Signature was signed by ${p1Key.id} whereas key is ${p2SigKey.id})"
          )
        )
      }
    }
    "accept valid transactions" in {
      val (manager, store, service) = generate(false)
      when(
        manager.add(
          any[SignedTopologyTransaction[TopologyChangeOp]],
          anyBoolean,
          anyBoolean,
          anyBoolean,
        )(
          anyTraceContext
        )
      )
        .thenReturn(EitherT.fromEither[Future](Right(())))
      verify(manager, never).authorize(
        any[TopologyTransaction[TopologyChangeOp]],
        any[Option[Fingerprint]],
        anyBoolean,
        anyBoolean,
      )(anyTraceContext) // should not auto enable
      for {
        res <- service.newRequest(List(p1Mapping, partyMapping))
      } yield {
        res should have length (2)
        res.map(_.state) shouldBe Seq(Accepted, Accepted)
      }
    }

    "correctly propagate topology manager rejects" in {
      val (manager, store, service) = generate(false)
      val reject =
        DomainTopologyManagerError.ParticipantNotInitialized.Failure(
          participant1,
          KeyCollection(Seq(), Seq()),
        )
      def answer(): EitherT[Future, DomainTopologyManagerError, Unit] =
        EitherT.fromEither[Future](Left(reject))

      when(
        manager.add(
          any[SignedTopologyTransaction[TopologyChangeOp]],
          anyBoolean,
          anyBoolean,
          anyBoolean,
        )(
          anyTraceContext
        )
      )
        .thenAnswer((_: SignedTopologyTransaction[TopologyChangeOp]) => answer())
      for {
        res <- service.newRequest(List(p1Mapping))
      } yield {
        res should have length (1)
        res.map(_.state) shouldBe Seq(Failed(reject.toString))
      }
    }

    "reject duplicates correctly" in {
      val (manager, store, service) = generate(false)
      for {
        _ <- addToStore(store, p1Mapping)
        res <- service.newRequest(List(p1Mapping))
      } yield {
        res.map(_.state) shouldBe Seq(Duplicate)
      }

    }
    "auto-enable new participants when enabled" in {
      val (manager, store, service) = generate(autoEnable = true)
      val trustCert = genSigned(
        TopologyStateUpdate.createAdd(
          ParticipantState(
            RequestSide.To,
            domainId,
            participant1,
            ParticipantPermission.Submission,
            TrustLevel.Ordinary,
          )
        ),
        p1Key,
      )
      def answer(
          x: SignedTopologyTransaction[TopologyChangeOp]
      ): EitherT[Future, DomainTopologyManagerError, Unit] =
        EitherT.right(addToStore(store, x))
      when(manager.addParticipant(any[ParticipantId], any[Option[X509Cert]]))
        .thenReturn(EitherT.rightT(()))
      when(
        manager.add(
          any[SignedTopologyTransaction[TopologyChangeOp]],
          anyBoolean,
          anyBoolean,
          anyBoolean,
        )(
          anyTraceContext
        )
      )
        .thenAnswer(x => answer(x))
      when(
        manager.authorize(
          any[TopologyTransaction[TopologyChangeOp]],
          any[Option[Fingerprint]],
          anyBoolean,
          anyBoolean,
        )(anyTraceContext)
      )
        .thenAnswer {
          (
              x: TopologyTransaction[TopologyChangeOp],
              _: Option[Fingerprint],
              _: Boolean,
              _: Boolean,
          ) =>
            val signed = genSigned(x, p1Key)
            answer(signed).map(_ => signed)
        }

      when(manager.store).thenReturn(store)
      for {
        res <- service.newRequest(List(p1Mapping, p1MappingEnc, trustCert))
        dis <- store.headTransactions.map(_.toTopologyState)
      } yield {
        res.map(_.state) shouldBe Seq(Accepted, Accepted, Accepted)
        dis should have length (4)
        assert(
          dis.map(_.mapping).exists {
            case ParticipantState(RequestSide.From, _, `participant1`, _, _) => true
            case _ => false
          },
          dis,
        )
      }
    }

  }

  "queue strategy should" should {
    def generate() = {
      val manager = mock[DomainTopologyManager]
      val queueStore = new InMemoryTopologyStore(loggerFactory)
      val strategy = new QueueStrategy(clock, queueStore, loggerFactory)
      val (_, store, service) = generateBase(false, strategy)
      (manager, store, service, queueStore)
    }
    "queue correctly" in {
      val (manager, store, service, queueStore) = generate()
      for {
        res <- service.newRequest(List(p1Mapping, p1MappingEnc))
        itm <- queueStore.allTransactions
      } yield {
        res.map(_.state) shouldBe Seq(Requested, Requested)
        itm.result should have length (2)
      }
    }
    "reject duplicates in queue" in {
      val (manager, store, service, queueStore) = generate()
      for {
        _ <- addToStore(queueStore, p1Mapping)
        res <- service.newRequest(List(p1Mapping, p1MappingEnc))
      } yield {
        res.map(_.state) shouldBe Seq(Duplicate, Requested)

      }
    }
  }

  "reject strategy" should {

    "reject everything" in {
      val (manager, store, service) = generateBase(false, new AutoRejectStrategy())
      for {
        res <- service.newRequest(List(p1Mapping, p1MappingEnc))
      } yield {
        res.map(_.state) shouldBe Seq(Rejected, Rejected)
      }
    }
  }

}
