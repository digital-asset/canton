// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{Signature, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.error.MediatorError.MalformedMessage
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  InformeeMessage,
  RootHashMessage,
  SerializedRootHashMessagePayload,
  Verdict,
}
import com.digitalasset.canton.protocol.{
  ExampleTransactionFactory,
  RequestId,
  TestSynchronizerParameters,
}
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MediatorGroupRecipient,
  MemberRecipient,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  MediatorGroup,
  MediatorId,
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.HasTestCloseContext.makeTestCloseContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
  config,
}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class DefaultVerdictSenderTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutionContext
    with BaseTest {

  private lazy implicit val testCloseContext: CloseContext = makeTestCloseContext(logger)
  private lazy val activeMediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator", "one"))
  private lazy val activeMediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator", "two"))
  private lazy val passiveMediator3 = MediatorId(UniqueIdentifier.tryCreate("mediator", "three"))

  private lazy val mediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero)
  private lazy val defaultMediatorGroup: MediatorGroup = MediatorGroup(
    index = mediatorGroupRecipient.group,
    active = NonEmpty(Seq, activeMediator1, activeMediator2),
    passive = Seq(
      passiveMediator3
    ),
    threshold = PositiveInt.tryCreate(2),
  )
  private lazy val expectedMediatorGroupAggregationRule = Some(
    AggregationRule.activeMediators(
      NonEmpty.mk(Seq, defaultMediatorGroup.active(0), defaultMediatorGroup.active.tail*),
      NonNegativeInt.zero,
      PositiveInt.tryCreate(2),
      testedProtocolVersion,
    )
  )

  "DefaultVerdictSender" should {
    "work for pv <= dev" should {
      "send approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for active mediators" should {
      "send approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
      "send rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }

    "for passive mediators" should {
      "not send approvals" in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
      "not send rejects" in {
        val tester = TestHelper(
          mediatorId = passiveMediator3,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 0
        }
      }
    }

    "for requests to a mediator group should set aggregation rule" should {
      "for approvals" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe expectedMediatorGroupAggregationRule
        }
      }
      "for rejects" in {
        val tester = TestHelper(
          mediatorId = activeMediator1,
          transactionMediatorGroup = mediatorGroupRecipient,
        )
        tester.sendReject() map { _ =>
          tester.interceptedMessages should have size 1
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          aggregationRule shouldBe expectedMediatorGroupAggregationRule
        }
      }
      "delay responses" in {
        // In this test, we set the threshold to 1. As the requestId is deterministic, we know
        // that mediator2 will be the "spare mediator" and as such should submit the response with
        // a delay of 250ms (as per configuration).
        val tester = TestHelper(
          mediatorId = activeMediator2,
          transactionMediatorGroup = mediatorGroupRecipient,
          mediatorGroup = defaultMediatorGroup.copy(threshold = PositiveInt.one),
        )
        when(
          tester.clock.scheduleAfterCancelledOnShutdown(
            any[CantonTimestamp => Unit],
            any[String],
            any[Duration],
          )(any[ExecutionContext], any[CloseContext])
        )
          .thenAnswer[CantonTimestamp => Unit, String, Duration] { case (_, _, duration) =>
            duration.toMillis shouldBe 250
            FutureUnlessShutdown.unit
          }
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
          verify(tester.clock, times(1)).scheduleAfterCancelledOnShutdown(
            any[CantonTimestamp => Unit],
            any[String],
            any[Duration],
          )(any[ExecutionContext], any[CloseContext])
          val (_, aggregationRule) = tester.interceptedMessages.loneElement
          if (testedProtocolVersion > ProtocolVersion.v34)
            aggregationRule shouldBe expectedMediatorGroupAggregationRule
          succeed
        }
      }
      "not delay responses if response deadline is approaching" in {
        // Very similar to above but here we assume that the response deadline is approaching,
        // so we fire using all cannons.
        val tester = TestHelper(
          mediatorId = activeMediator2,
          transactionMediatorGroup = mediatorGroupRecipient,
          mediatorGroup = defaultMediatorGroup.copy(threshold = PositiveInt.one),
          immediateBeforeDeadline =
            NonNegativeLong.tryCreate(120L), // request / now is epoch, deadline is epoch + 120s
        )
        tester.sendApproval() map { _ =>
          tester.interceptedMessages should have size 1
        }
      }
    }
  }

  private case class TestHelper(
      mediatorId: MediatorId,
      transactionMediatorGroup: MediatorGroupRecipient,
      mediatorGroup: MediatorGroup = defaultMediatorGroup,
      immediateBeforeDeadline: NonNegativeLong = NonNegativeLong.zero,
  ) {

    val psid: PhysicalSynchronizerId = SynchronizerId(
      UniqueIdentifier.tryFromProtoPrimitive("synchronizer::test")
    ).toPhysical
    val testTopologyTimestamp = CantonTimestamp.Epoch

    val factory =
      new ExampleTransactionFactory()(
        psid = psid,
        mediatorGroup = transactionMediatorGroup,
      )
    val mediatorRecipient: MediatorGroupRecipient = factory.mediatorGroup
    val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
    val informeeMessage =
      InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
    val rootHashMessage = RootHashMessage(
      fullInformeeTree.updateId.toRootHash,
      psid,
      ViewType.TransactionViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
    val participant: ParticipantId = ExampleTransactionFactory.submittingParticipant
    val rhmEnvelope = OpenEnvelope(
      rootHashMessage,
      Recipients.cc(transactionMediatorGroup, MemberRecipient(participant)),
    )(testedProtocolVersion)

    val submitter = ExampleTransactionFactory.submitter
    val signatory = ExampleTransactionFactory.signatory
    val observer = ExampleTransactionFactory.observer

    val requestIdTs = CantonTimestamp.Epoch
    val requestId = RequestId(requestIdTs)
    val decisionTime = requestIdTs.plusSeconds(120)
    val clock = mock[Clock]
    when(clock.now).thenReturn(requestIdTs.plusSeconds(1))

    val initialSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

    val synchronizerSyncCryptoApi: SynchronizerCryptoClient =
      if (testedProtocolVersion >= ProtocolVersion.v34) {
        val topology = TestingTopology.from(
          Set(psid),
          Map(
            submitter -> Map(participant -> ParticipantPermission.Confirmation),
            signatory ->
              Map(participant -> ParticipantPermission.Confirmation),
            observer ->
              Map(participant -> ParticipantPermission.Observation),
          ),
          Set(mediatorGroup),
        )

        val identityFactory = TestingIdentityFactory(
          topology,
          loggerFactory,
          dynamicSynchronizerParameters = initialSynchronizerParameters,
        )

        identityFactory.forOwnerAndSynchronizer(mediatorId, psid)
      } else {
        val topology = TestingTopology.from(
          Set(psid),
          Map(
            submitter -> Map(participant -> ParticipantPermission.Confirmation),
            signatory ->
              Map(participant -> ParticipantPermission.Confirmation),
            observer ->
              Map(participant -> ParticipantPermission.Observation),
          ),
          Set(
            MediatorGroup(
              MediatorGroupIndex.zero,
              NonEmpty(Seq, mediatorId),
              Seq.empty,
              PositiveInt.one,
            )
          ),
        )

        val identityFactory = TestingIdentityFactory(
          topology,
          loggerFactory,
          dynamicSynchronizerParameters = initialSynchronizerParameters,
        )

        identityFactory.forOwnerAndSynchronizer(mediatorId, psid)
      }

    private val sequencerClientSend: TestSequencerClientSend = new TestSequencerClientSend(
      clock
    )

    def interceptedMessages: Seq[(Batch[DefaultOpenEnvelope], Option[AggregationRule])] =
      sequencerClientSend.requestsQueue.asScala.map { request =>
        (request.batch, request.aggregationRule)
      }.toSeq

    val verdictSender = new DefaultVerdictSender(
      sequencerClientSend,
      synchronizerSyncCryptoApi,
      mediatorId,
      VerdictSenderParameters(
        enableDelay = true,
        livenessMargin = NonNegativeInt.zero,
        immediateBeforeDeadline =
          config.NonNegativeFiniteDuration.ofSeconds(immediateBeforeDeadline.value),
        initialDelay = config.NonNegativeFiniteDuration.ofMillis(250),
        delay = config.NonNegativeFiniteDuration.ofMillis(100),
        parallelism = PositiveInt.two,
      ),
      loggerFactory,
    )

    def sendApproval(): Future[Unit] =
      verdictSender
        .sendResult(
          requestId,
          informeeMessage,
          Verdict.Approve(testedProtocolVersion),
          decisionTime,
        )
        .onShutdown(fail())

    def sendReject(): Future[Unit] =
      verdictSender
        .sendReject(
          requestId,
          Some(informeeMessage),
          Seq(rhmEnvelope),
          MediatorVerdict
            .MediatorReject(MalformedMessage.Reject("Test failure"))
            .toVerdict(testedProtocolVersion),
          decisionTime,
        )
        .failOnShutdown
  }

}
