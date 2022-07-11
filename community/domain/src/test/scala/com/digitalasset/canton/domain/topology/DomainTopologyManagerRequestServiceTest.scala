// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{
  Attack,
  SecurityTest,
  SecurityTestLayer,
  SecurityTestSuite,
}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.config.TopologyConfig
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse.State._
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, blocking}

class DomainTopologyManagerRequestServiceTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with SecurityTestSuite
    with HasExecutionContext {

  override val securityTestLayer: SecurityTestLayer = SecurityTestLayer.KeyRequirements

  lazy val factory = new TopologyTransactionTestFactory(
    loggerFactory,
    parallelExecutionContext,
  )
  lazy val unauthenticatedMember = UnauthenticatedMemberId(DefaultTestIdentities.uid)
  class Fixture {

    val authorizedStore = new InMemoryTopologyStore(AuthorizedStore, loggerFactory)
    val uniqueTs = new AtomicReference[CantonTimestamp](CantonTimestamp.Epoch)

    def append(txs: SignedTopologyTransaction[TopologyChangeOp]*): Future[Unit] = {
      val ts = uniqueTs.getAndUpdate(_.plusMillis(1))
      authorizedStore.append(
        SequencedTime(ts),
        EffectiveTime(ts),
        txs.map(ValidatedTopologyTransaction(_, None)),
      )
    }
    def storedTransactions: Future[Seq[SignedTopologyTransaction[TopologyChangeOp]]] =
      authorizedStore.allTransactions.map(_.result.map(_.transaction))

    val fromResponses =
      mutable.Queue[EitherT[Future, DomainTopologyManagerError, Unit]]()

    val trustParticipantResponses =
      mutable.Queue[EitherT[Future, DomainTopologyManagerError, Unit]]()
    val hooks = new RequestProcessingStrategy.ManagerHooks() {
      override def addFromRequest(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
          traceContext: TraceContext
      ): EitherT[Future, DomainTopologyManagerError, Unit] = blocking {
        synchronized {
          (if (fromResponses.nonEmpty) fromResponses.front
           else EitherT.rightT[Future, DomainTopologyManagerError](())).flatMap(_ =>
            // if the response is positive, we append the transaction to the authorized store
            EitherT.right(append(transaction))
          )
        }
      }
      override def addParticipant(participantId: ParticipantId, x509: Option[X509Cert])(implicit
          traceContext: TraceContext
      ): EitherT[Future, DomainTopologyManagerError, Unit] = EitherT.rightT(())
      override def issueParticipantStateForDomain(participantId: ParticipantId)(implicit
          traceContext: TraceContext
      ): EitherT[Future, DomainTopologyManagerError, Unit] = blocking {
        synchronized {
          (if (trustParticipantResponses.nonEmpty) trustParticipantResponses.front
           else EitherT.rightT[Future, DomainTopologyManagerError](())).flatMap { _ =>
            EitherT.right[DomainTopologyManagerError](
              append(
                factory.mkAdd(
                  ParticipantState(
                    RequestSide.From,
                    factory.domainId,
                    participantId,
                    ParticipantPermission.Submission,
                    TrustLevel.Ordinary,
                  )
                )
              )
            )
          }
        }
      }
    }

    val client = mock[DomainTopologyClient]
    when(client.await(any[TopologySnapshot => Future[Boolean]], any[Duration])(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.pure(true))
    def service(config: TopologyConfig = TopologyConfig()): DomainTopologyManagerRequestService = {
      new DomainTopologyManagerRequestService(
        new RequestProcessingStrategy.Impl(
          config = config,
          domainId = DefaultTestIdentities.domainId,
          protocolVersion = testedProtocolVersion,
          authorizedStore = authorizedStore,
          targetDomainClient = client,
          hooks,
          timeouts = DefaultProcessingTimeouts.testing,
          loggerFactory = loggerFactory,
        ),
        factory.cryptoApi.crypto.pureCrypto,
        loggerFactory,
      )
    }

  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {}
  }

  override type FixtureParam = Fixture

  /*
   *   Ignore duplicate requests
   */

  private def all(
      res: Seq[RegisterTopologyTransactionResponse.Result],
      len: Long,
      status: RegisterTopologyTransactionResponse.State,
  ) = {
    res should have length len
    forAll(res.map(_.state)) {
      case `status` => succeed
      case _ => fail(s"should be $status")
    }
  }

  private def onboardingTests(config: TopologyConfig) = {

    def expectMalicious[A](within: => A) = {
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        within,
        messages =>
          forAll(messages) { entry =>
            entry.warningMessage should (include("request as participant is not active")
              or include(InvalidOrFaultyOnboardingRequest.id))
          },
      )
    }

    lazy val securityAsset: SecurityTest =
      SecurityTest(
        property = Authorization,
        asset = s"participant on-boarding to ${if (config.open) "open" else "permissioned"} domain",
      )

    "onboard new participants" taggedAs securityAsset.setHappyCase("onboard new participants") in {
      implicit f =>
        import factory._
        val service = f.service(config)
        for {
          // store allow list certificate if domain is permissioned
          _ <- if (config.open) Future.unit else f.append(ps1d1F_k1)
          request <- service.newRequest(
            unauthenticatedMember,
            participant1,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
          )
          stored <- f.storedTransactions
        } yield {
          all(request, 6, Accepted)
          stored should have length 7
        }
    }

    "reject invalid onboarding transactions" taggedAs securityAsset.setAttack(
      Attack(
        actor = "malicious or faulty participant",
        threat = "register insufficient or invalid topology transactions",
        mitigation = "reject onboarding request",
      )
    ) in { implicit f =>
      import factory._
      val service = f.service(config)
      for {
        // store allow list certificate if domain is permissioned
        _ <-
          if (config.open) Future.unit
          else
            f.append(
              ps1d1F_k1
            ) // add cert so we don't fail at this specific test, as this is tested elsewhere
        // wrong participant id
        request <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant6,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
          )
        )
        // not as unauthenticated member
        request2 <- expectMalicious(
          service.newRequest(
            participant1,
            participant1,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
          )
        )
        // missing namespace delegations
        request3 <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant1,
            List(ns1k1_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
          )
        )
        // missing signing key
        request4 <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant1,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak1E_k3, ps1d1T_k3),
          )
        )
        // missing encryption key
        request5 <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant1,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, ps1d1T_k3),
          )
        )
        // missing domain trust certificate
        request6 <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant1,
            List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak1E_k3, okm1ak5_k3),
          )
        )
        // excess transactions only rejected since PV=3
        request7 <-
          expectMalicious(
            service.newRequest(
              unauthenticatedMember,
              participant1,
              List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak1E_k3, okm1ak5_k3, ps1d1T_k3, p1p1B_k2),
            )
          )
        // bad signatures
        request8 <- expectMalicious(
          service.newRequest(
            unauthenticatedMember,
            participant1,
            List(
              ns1k1_k1,
              ns1k2_k1,
              ns1k3_k2,
              okm1ak1E_k3,
              okm1ak5_k3,
              ps1d1T_k3.copy(signature = okm1ak5_k3.signature)(
                ps1d1T_k3.representativeProtocolVersion,
                None,
              ),
            ),
          )
        )
        stored <- f.storedTransactions
      } yield {
        all(request, 6, Failed)
        all(request2, 6, Rejected)
        all(request3, 5, Failed)
        all(request4, 5, Failed)
        all(request5, 5, Failed)
        all(request6, 5, Failed)
        all(request8, 6, Failed)
        // excess transactions only rejected since PV=3
        if (testedProtocolVersion >= ProtocolVersion.v3_0_0) {
          all(request7, 7, Failed)
          stored should have length (if (config.open) 0 else 1)
        } else {
          succeed
        }
      }
    }

    "accept updates from existing participants" in { implicit f =>
      import factory._
      val service = f.service(config)
      for {
        _ <- f.append(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3, ps1d1F_k1)
        request <- service.newRequest(participant1, participant1, List(p1p1B_k2))
        stored <- f.storedTransactions
      } yield {
        all(request, 1, Accepted)
        stored should have length 8
      }
    }

    "reject changes from inactive participants" taggedAs securityAsset.setAttack(
      Attack(
        actor = "deactivated participants",
        threat = "attempting to manipulate topology state",
        mitigation = "reject request",
      )
    ) in { implicit f =>
      import factory._
      val service = f.service(config)
      for {
        _ <- f.append(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3)
        request <- loggerFactory.assertLogs(
          service.newRequest(participant1, participant1, List(p1p1B_k2)),
          _.warningMessage should include("not active"),
        )
        stored <- f.storedTransactions
      } yield {
        all(request, 1, Rejected)
        stored should have length 6
      }
    }

    "ignore duplicates" in { implicit f =>
      import factory._
      val service = f.service(config)
      for {
        _ <- f.append(ns1k1_k1, ns1k2_k1, ns1k3_k2, ps1d1F_k1)
        request <- service.newRequest(
          unauthenticatedMember,
          participant1,
          List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
        )
      } yield {
        request.map(_.state) shouldBe Seq(
          Duplicate,
          Duplicate,
          Duplicate,
          Accepted,
          Accepted,
          Accepted,
        )
      }
    }

  }

  "open domains" should {
    onboardingTests(TopologyConfig(open = true))
  }

  "permissioned domains" should {
    onboardingTests(TopologyConfig(open = false))

    "reject participants not on the allow-list" taggedAs SecurityTest(
      property = Authorization,
      asset = s"participant on-boarding to permissioned domain",
    ).setAttack(
      Attack(
        actor = "untrusted participant",
        threat = "attempting to join the domain",
        mitigation = "reject request",
      )
    ) in { implicit f =>
      import factory._
      val service = f.service(TopologyConfig(open = false))
      for {
        request <- service.newRequest(
          unauthenticatedMember,
          participant1,
          List(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3, okm1ak1E_k3, ps1d1T_k3),
        )
      } yield {
        all(request, 6, Rejected)
      }
    }
  }
}
