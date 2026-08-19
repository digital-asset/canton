// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{
  NamedLoggerFactory,
  NamedLogging,
  SuppressionRule,
  TracedLogger,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerNetworkStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultSendBlacklistTtl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.memory.GenericInMemoryP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  WorkflowId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  endpointToTestBftNodeId,
  endpointToTestSequencerId,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.mockito.captor.ArgCaptor
import org.scalatest.Assertions.fail
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level
import shapeless.*
import shapeless.HList.*
import shapeless.syntax.std.traversable.*

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Random, Success, Try}

class P2PNetworkOutModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  import P2PNetworkOutModuleTest.*

  implicit val pv: ProtocolVersion = testedProtocolVersion

  private val simClock = new SimClock(loggerFactory = loggerFactory)

  "p2p output" when {
    "ready" should {
      "connect to nodes and " +
        "initialize availability and " +
        "consensus once enough nodes are connected if starting from genesis" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val availabilitySpy =
            spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
          val consensusSpy =
            spy(fakeIgnoringModule[Consensus.Message[ProgrammableUnitTestEnv]])
          val outputSpy =
            spy(fakeIgnoringModule[Output.Message[ProgrammableUnitTestEnv]])
          val pruningSpy =
            spy(fakeIgnoringModule[Pruning.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithDefaultDepsExpectingSilence(
              mempool = mempoolSpy,
              availability = availabilitySpy,
              consensus = consensusSpy,
              output = outputSpy,
              pruning = pruningSpy,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          import state.*

          // No other node is authenticated
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 1
          mempoolStarted shouldBe true
          availabilityStarted shouldBe false
          consensusStarted shouldBe false
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(mempoolSpy, times(1)).asyncSend(Mempool.Start)
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
          verify(outputSpy, times(1)).asyncSend(Output.Start)
          verify(pruningSpy, times(1)).asyncSend(Pruning.Start)
          verify(availabilitySpy, never).asyncSend(
            any[Availability.Message[ProgrammableUnitTestEnv]]
          )(any[TraceContext], any[MetricsContext])
          verify(consensusSpy, never).asyncSend(any[Consensus.Message[ProgrammableUnitTestEnv]])(
            any[TraceContext],
            any[MetricsContext],
          )

          // One node authenticates -> weak quorum reached
          connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)

          context.selfMessages should contain theSameElementsInOrderAs
            Seq[P2PNetworkOut.Network](
              P2PNetworkOut.Network.Connected(Some(otherInitialEndpointsTupled._1.id)),
              P2PNetworkOut.Network
                .Authenticated(
                  endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
                  Some(otherInitialEndpointsTupled._1),
                ),
            )
          context.extractSelfMessages().foreach(module.receive)
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections.toMap
            .updated(
              Some(otherInitialEndpointsTupled._1.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 2
          mempoolStarted shouldBe true
          availabilityStarted shouldBe true
          consensusStarted shouldBe false
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, never).asyncSend(Consensus.Start)

          // One more nodes authenticated -> strong quorum reached
          connect(p2pNetworkManager, otherInitialEndpointsTupled._2)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._2)

          context.selfMessages should contain theSameElementsInOrderAs
            Seq[P2PNetworkOut.Network](
              P2PNetworkOut.Network.Connected(Some(otherInitialEndpointsTupled._2.id)),
              P2PNetworkOut.Network.Authenticated(
                endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
                Some(otherInitialEndpointsTupled._2),
              ),
            )
          context.extractSelfMessages().foreach(module.receive)
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 3))
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections.toMap
            .updated(
              Some(otherInitialEndpointsTupled._1.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
            .updated(
              Some(otherInitialEndpointsTupled._2.id),
              Some(endpointToTestBftNodeId(otherInitialEndpointsTupled._2)),
            )
          initialNodesConnecting shouldBe true
          maxNodesContemporarilyAuthenticated shouldBe 3
          mempoolStarted shouldBe true
          availabilityStarted shouldBe true
          consensusStarted shouldBe true
          outputStarted shouldBe true
          pruningStarted shouldBe true
          verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
          verify(consensusSpy, times(1)).asyncSend(Consensus.Start)
        }

      "initialize availability and consensus immediately if NOT starting from genesis" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val availabilitySpy =
          spy(fakeIgnoringModule[Availability.Message[ProgrammableUnitTestEnv]])
        val consensusSpy =
          spy(fakeIgnoringModule[Consensus.Message[ProgrammableUnitTestEnv]])
        val outputSpy =
          spy(fakeIgnoringModule[Output.Message[ProgrammableUnitTestEnv]])
        val pruningSpy =
          spy(fakeIgnoringModule[Pruning.Message])
        val (_, state, _, _) =
          setupWithDefaultDepsExpectingSilence(
            mempool = mempoolSpy,
            availability = availabilitySpy,
            consensus = consensusSpy,
            output = outputSpy,
            pruning = pruningSpy,
            isGenesis = false,
          )

        import state.*

        // No other node is authenticated
        p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections
        initialNodesConnecting shouldBe true
        maxNodesContemporarilyAuthenticated shouldBe 1
        mempoolStarted shouldBe true
        availabilityStarted shouldBe true
        consensusStarted shouldBe true
        outputStarted shouldBe true
        pruningStarted shouldBe true
        verify(mempoolSpy, times(1)).asyncSend(Mempool.Start)
        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        verify(availabilitySpy, times(1)).asyncSend(Availability.Start)
        verify(consensusSpy, times(1)).asyncSend(Consensus.Start)
        verify(outputSpy, times(1)).asyncSend(Output.Start)
        verify(pruningSpy, times(1)).asyncSend(Pruning.Start)
      }
    }

    "is requested to multicast a network message and " +
      "all nodes are authenticated" should {
        "send the message to all nodes" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)
          connect(p2pNetworkManager, otherInitialEndpointsTupled._2)
          authenticate(p2pNetworkManager, otherInitialEndpointsTupled._2)
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating all nodes

          val authenticatedEndpoints =
            Set(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2)
          val nodes = authenticatedEndpoints.map(endpointToTestBftNodeId)

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              nodes,
            )
          )

          authenticatedEndpoints.foreach(
            verify(sendActionSpy, times(1)).apply(
              _,
              BftOrderingMessage(
                "",
                Some(networkMessageBody),
                selfNode,
                None,
              ),
            )
          )
        }
      }

    "is requested to multicast a network message and " +
      "only some nodes are authenticated" should {
        "send the message only to authenticated nodes" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating all nodes

          val node = endpointToTestBftNodeId(otherInitialEndpointsTupled._1)

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              Set(node),
            )
          )

          val networkSend =
            BftOrderingMessage(
              "",
              Some(networkMessageBody),
              selfNode,
              None,
            )
          verify(sendActionSpy, times(1)).apply(
            otherInitialEndpointsTupled._1,
            networkSend,
          )
          verify(sendActionSpy, times(1)).apply(
            any[P2PEndpoint],
            any[BftOrderingMessage],
          )
        }
      }

    "is requested to multicast a network message to self" should {
      "forward it directly to the P2P network in module" in {
        val sendActionSpy =
          spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
        val p2pNetworkInSpy = spy(fakeIgnoringModule[BftOrderingMessage])
        val (context, _, module, _) = setupWithIgnoringDefaultDeps(sendActionSpy, p2pNetworkInSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
        module.receive(
          P2PNetworkOut.Multicast(
            P2PNetworkOut.BftOrderingNetworkMessage.Empty,
            Set(selfNode),
          )
        )

        verify(sendActionSpy, never).apply(
          any[P2PEndpoint],
          any[BftOrderingMessage],
        )
        verify(p2pNetworkInSpy, times(1)).asyncSend(
          BftOrderingMessage(
            traceContext = "",
            Some(networkMessageBody),
            selfNode,
            None,
          )
        )
      }
    }

    "is requested to send a network message to a number of random authenticated peers among a set of possible recipients" should {

      "do it" when {
        "at least one of the possible recipients is authenticated" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(
            otherInitialEndpointsTupled._1,
            otherInitialEndpointsTupled._2,
            otherInitialEndpointsTupled._3,
          ).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating all nodes

          val otherNodeEndpoint1 = otherInitialEndpointsTupled._1
          val otherNodeId1 = endpointToTestBftNodeId(otherNodeEndpoint1)
          val otherNodeEndpoint2 = otherInitialEndpointsTupled._2
          val otherNodeId2 = endpointToTestBftNodeId(otherNodeEndpoint2)
          val possibleRecipients =
            Seq(otherNodeId1, otherNodeId2, endpointToTestBftNodeId(anotherEndpoint))

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              firstChoiceRecipientsPool = possibleRecipients,
              secondChoiceRecipientsPool = None,
              howManyRecipients = PositiveInt.tryCreate(2),
            )
          )

          verify(sendActionSpy, times(1)).apply(
            eqTo(otherNodeEndpoint1),
            eqTo(
              BftOrderingMessage(
                "",
                Some(networkMessageBody),
                sentBy = selfNode,
                sentAt = None,
              )
            ),
          )
          verify(sendActionSpy, times(1)).apply(
            eqTo(otherNodeEndpoint2),
            eqTo(
              BftOrderingMessage(
                "",
                Some(networkMessageBody),
                sentBy = selfNode,
                sentAt = None,
              )
            ),
          )
        }
      }

      "do nothing" when {
        "none among the possible recipients is authenticated" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, _) = setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val possibleRecipients =
            Seq(
              endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
              endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
              endpointToTestBftNodeId(anotherEndpoint),
            )

          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              firstChoiceRecipientsPool = possibleRecipients,
              secondChoiceRecipientsPool = None,
              howManyRecipients = PositiveInt.tryCreate(2),
            )
          )

          verify(sendActionSpy, never).apply(
            any[P2PEndpoint],
            any[BftOrderingMessage],
          )
        }
      }

      "blacklist peers" when {
        "they get retried for a given workflow" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)
          val onRecipientDecisionSpy = spyLambda((_: Seq[BftNodeId]) => ())

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val possibleRecipients = Seq(
            endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
            endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
            endpointToTestBftNodeId(otherInitialEndpointsTupled._3),
          )

          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              possibleRecipients,
              secondChoiceRecipientsPool = None,
              Some(WorkflowId("test-workflow")),
              nodesThatFailed = Seq.empty,
              Some(onRecipientDecisionSpy),
              howManyRecipients = PositiveInt.tryCreate(2),
            )
          )

          // The `onRecipientDecision` callback is also called when there's no authenticated peer
          verify(onRecipientDecisionSpy, times(1)).apply(Seq.empty)

          reset(onRecipientDecisionSpy)

          Seq(
            otherInitialEndpointsTupled._1,
            otherInitialEndpointsTupled._2,
            otherInitialEndpointsTupled._3,
          ).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating

          val networkMessageBody = BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)
          val p2pEndpointCaptor = ArgCaptor[P2PEndpoint]
          var initial = true
          for (
            nodesThatTimedOutF <- Seq[() => Seq[BftNodeId]](
              // First send request successful
              () => Seq.empty,
              // Same nodes blacklisted at second send request that were sent to successfully at first send request
              () => p2pEndpointCaptor.values.map(endpointToTestBftNodeId),
              // Third send request successful
              () => Seq.empty,
            )
          ) {
            module.receive(
              P2PNetworkOut.SendToRandomAuthenticated(
                P2PNetworkOut.BftOrderingNetworkMessage.Empty,
                possibleRecipients,
                secondChoiceRecipientsPool = None,
                Some(WorkflowId("test-workflow")),
                nodesThatTimedOutF.apply(),
                Some(onRecipientDecisionSpy),
                howManyRecipients = PositiveInt.tryCreate(2),
              )
            )
            // After blacklisting the 2 recipients in the first send we are left with only 1 viable recipient
            val sendsCount = if (initial) 2 else 1
            verify(sendActionSpy, times(sendsCount)).apply(
              if (initial)
                // Any endpoint in the candidate pool, and we capture them to blacklist them in the next send request
                p2pEndpointCaptor.capture
              else
                // Any other endpoint
                argThat((e: P2PEndpoint) => !p2pEndpointCaptor.values.contains(e)),
              eqTo(
                BftOrderingMessage(
                  "",
                  Some(networkMessageBody),
                  sentBy = selfNode,
                  sentAt = None,
                )
              ),
            )
            // The notification callback is always called
            verify(onRecipientDecisionSpy, times(1)).apply(any[Seq[BftNodeId]])

            reset(sendActionSpy)
            reset(onRecipientDecisionSpy)
            initial = false
          }

          // Nodes that were sent to successfully at the first send request are blacklisted for the workflow
          module.state.workflowBlacklists
            .get(WorkflowId("test-workflow"))
            .map(_.keys.toSet) shouldBe Some(
            p2pEndpointCaptor.values.map(endpointToTestBftNodeId).toSet
          )
        }
      }

      "fall back to second choice recipients and then blacklisted nodes" in {
        val sendActionSpy =
          spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
        val (context, _, module, p2pNetworkManager) =
          setupWithIgnoringDefaultDeps(sendActionSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        val endpoint1 = otherInitialEndpointsTupled._1
        val bftNodeId1 = endpointToTestBftNodeId(endpoint1)
        connect(p2pNetworkManager, endpoint1)
        authenticate(p2pNetworkManager, endpoint1)
        context.extractSelfMessages().foreach(module.receive) // Simulate authenticating

        val endpoint2 = otherInitialEndpointsTupled._2
        val bftNodeId2 = endpointToTestBftNodeId(endpoint2)

        val endpoint3 = otherInitialEndpointsTupled._3
        val bftNodeId3 = endpointToTestBftNodeId(endpoint3)

        Seq(endpoint2, endpoint3).foreach { e =>
          connect(p2pNetworkManager, e)
          authenticate(p2pNetworkManager, e)
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating
        }

        val firstChoiceRecipientsPool = Seq(bftNodeId1, bftNodeId2)
        val secondChoiceRecipientsPool = Some(Seq(endpointToTestBftNodeId(endpoint3)))

        // Blacklist a first choice and a second choice immediately
        module.receive(
          P2PNetworkOut.SendToRandomAuthenticated(
            P2PNetworkOut.BftOrderingNetworkMessage.Empty,
            firstChoiceRecipientsPool,
            secondChoiceRecipientsPool,
            Some(WorkflowId("test-workflow")),
            nodesThatFailed = Seq(bftNodeId1, bftNodeId3),
            howManyRecipients = PositiveInt.tryCreate(3),
          )
        )

        Seq(endpoint1, endpoint2, endpoint3).foreach { e =>
          verify(sendActionSpy, times(1)).apply(
            e,
            BftOrderingMessage(
              "",
              Some(BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)),
              sentBy = selfNode,
              sentAt = None,
            ),
          )
        }
        succeed
      }

      "ignore blacklists" when {
        "they cause the candidate selection pool to be empty" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val endpoint1 = otherInitialEndpointsTupled._1
          val bftNodeId1 = endpointToTestBftNodeId(endpoint1)
          connect(p2pNetworkManager, endpoint1)
          authenticate(p2pNetworkManager, endpoint1)
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating

          val firstChoiceRecipientsPool = Seq(endpointToTestBftNodeId(endpoint1))

          // Blacklist the first choice immediately without second choice,
          //  blacklist should be ignored since it is the only candidate
          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              firstChoiceRecipientsPool,
              secondChoiceRecipientsPool = None,
              Some(WorkflowId("test-workflow")),
              nodesThatFailed = Seq(bftNodeId1),
              howManyRecipients = PositiveInt.tryCreate(2),
            )
          )

          verify(sendActionSpy, times(1)).apply(
            endpoint1,
            BftOrderingMessage(
              "",
              Some(BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)),
              sentBy = selfNode,
              sentAt = None,
            ),
          )
          reset(sendActionSpy)

          val endpoint2 = otherInitialEndpointsTupled._2
          val bftNodeId2 = endpointToTestBftNodeId(endpoint2)
          connect(p2pNetworkManager, endpoint2)
          authenticate(p2pNetworkManager, endpoint2)
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating

          val secondChoiceRecipientsPool = Seq(bftNodeId2)

          loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.INFO))(
            // Blacklist the second choice immediately, both blacklists should be ignored
            module.receive(
              P2PNetworkOut.SendToRandomAuthenticated(
                P2PNetworkOut.BftOrderingNetworkMessage.Empty,
                firstChoiceRecipientsPool,
                secondChoiceRecipientsPool = Some(secondChoiceRecipientsPool),
                Some(WorkflowId("test-workflow")),
                nodesThatFailed = Seq(bftNodeId2),
                howManyRecipients = PositiveInt.tryCreate(2),
              )
            ),
            _.exists(
              _.infoMessage.contains(
                "Not enough authenticated and whitelisted nodes available"
              )
            ) shouldBe true,
          )

          verify(sendActionSpy, times(1)).apply(
            endpoint1,
            BftOrderingMessage(
              "",
              Some(BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)),
              sentBy = selfNode,
              sentAt = None,
            ),
          )
          verify(sendActionSpy, times(1)).apply(
            endpoint2,
            BftOrderingMessage(
              "",
              Some(BftOrderingMessageBody(BftOrderingMessageBody.Message.Empty)),
              sentBy = selfNode,
              sentAt = None,
            ),
          )
        }
      }

      "do not send" when {
        "no authenticated nodes are in either pool" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val firstChoiceRecipientsPool = Seq(
            endpointToTestBftNodeId(otherInitialEndpointsTupled._1)
          )
          val secondChoiceRecipientsPool = Some(
            Seq(endpointToTestBftNodeId(otherInitialEndpointsTupled._2))
          )

          // Authenticate another node
          val endpoint3 = otherInitialEndpointsTupled._3
          connect(p2pNetworkManager, endpoint3)
          authenticate(p2pNetworkManager, endpoint3)
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating

          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              firstChoiceRecipientsPool,
              secondChoiceRecipientsPool,
              Some(WorkflowId("test-workflow")),
              nodesThatFailed = Seq.empty,
              howManyRecipients = PositiveInt.tryCreate(2),
            )
          )

          verify(sendActionSpy, never).apply(
            any[P2PEndpoint],
            any[BftOrderingMessage],
          )
        }
      }

      "remove blacklists" when {
        "they expire" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating all nodes

          val possibleRecipients = Seq(
            endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
            endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
          )

          for (
            nodesThatTimedOut <- Seq(
              Seq.empty,
              Seq(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
          ) {
            module.receive(
              P2PNetworkOut.SendToRandomAuthenticated(
                P2PNetworkOut.BftOrderingNetworkMessage.Empty,
                possibleRecipients,
                secondChoiceRecipientsPool = None,
                Some(WorkflowId("test-workflow")),
                nodesThatFailed = nodesThatTimedOut,
              )
            )
          }

          simClock.advance(DefaultSendBlacklistTtl.plus(1.second).toJava)

          // Trigger expiration check by sending a new message
          module.receive(
            P2PNetworkOut.SendToRandomAuthenticated(
              P2PNetworkOut.BftOrderingNetworkMessage.Empty,
              possibleRecipients,
              secondChoiceRecipientsPool = None,
              Some(WorkflowId("test-workflow")),
              nodesThatFailed = Seq.empty,
            )
          )

          module.state.workflowBlacklists.get(WorkflowId("test-workflow")) shouldBe Some(
            Map.empty
          )
        }
      }

      "clean all blacklist info" when {
        "requested to end a workflow" in {
          val sendActionSpy =
            spyLambda((_: P2PEndpoint, _: BftOrderingMessage) => ())
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(sendActionSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          Seq(otherInitialEndpointsTupled._1, otherInitialEndpointsTupled._2).foreach { e =>
            connect(p2pNetworkManager, e)
            authenticate(p2pNetworkManager, e)
          }
          context.extractSelfMessages().foreach(module.receive) // Simulate authenticating all nodes

          val possibleRecipients = Seq(
            endpointToTestBftNodeId(otherInitialEndpointsTupled._1),
            endpointToTestBftNodeId(otherInitialEndpointsTupled._2),
          )

          for (
            nodesThatTimedOut <- Seq(
              Seq.empty,
              Seq(endpointToTestBftNodeId(otherInitialEndpointsTupled._1)),
            )
          )
            module.receive(
              P2PNetworkOut.SendToRandomAuthenticated(
                P2PNetworkOut.BftOrderingNetworkMessage.Empty,
                possibleRecipients,
                secondChoiceRecipientsPool = None,
                Some(WorkflowId("test-workflow")),
                nodesThatTimedOut,
              )
            )

          module.receive(
            P2PNetworkOut.EndWorkflow(WorkflowId("test-workflow"))
          )

          module.state.workflowBlacklists.get(WorkflowId("test-workflow")) shouldBe None
        }
      }
    }

    "it is requested to add an endpoint" should {

      "add and connect the new endpoint" when {
        "the endpoint is not already stored nor connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val newEndpoint = anotherEndpoint

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              newEndpoint,
              added => endpointAdded = added,
            )
          )

          import state.*

          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          // Store and connect to endpoint
          context.runPipedMessagesThenVerifyAndReceiveOnModule(module) { message =>
            message shouldBe P2PNetworkOut.Internal.Connect(anotherEndpoint)
          }
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs otherInitialEndpoints.map(
            _ -> None
          ) :+ (anotherEndpoint -> None)

          endpointAdded shouldBe true
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections :+ Some(
            newEndpoint.id
          ) -> None

          authenticate(p2pNetworkManager, newEndpoint)
          context.extractSelfMessages().foreach(module.receive)

          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      "connect the endpoint" when {
        "it is already stored but not yet connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val p2pEndpointsStore =
            new InMemoryUnitTestP2PEndpointsStore(otherInitialEndpoints.toSet)
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(
              mempool = mempoolSpy,
              p2pEndpointsStore = p2pEndpointsStore,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          val newEndpoint = anotherEndpoint
          p2pEndpointsStore.addEndpoint(newEndpoint).apply() // Endpoint already in store

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              newEndpoint,
              added => endpointAdded = added,
            )
          )

          context.runPipedMessages() should contain only P2PNetworkOut.Internal.Connect(
            anotherEndpoint
          )
          endpointAdded shouldBe false
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      "just store the endpoint" when {
        "it is not stored but it is connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          p2pNetworkManager.createNetworkRef(
            ctx,
            P2PAddress.Endpoint(anotherEndpoint),
          ) // Endpoint already connected

          connect(p2pNetworkManager, anotherEndpoint)
          authenticate(p2pNetworkManager, anotherEndpoint)
          context
            .extractSelfMessages()
            .foreach(module.receive) // Simulate authenticating the endpoint

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              anotherEndpoint,
              added => endpointAdded = added,
            )
          )

          context.runPipedMessages() shouldBe empty
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs otherInitialEndpoints.map(
            _ -> None
          ) :+ (anotherEndpoint -> None)

          endpointAdded shouldBe true
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections :+ Some(
            anotherEndpoint.id
          ) -> Some(endpointToTestBftNodeId(anotherEndpoint))

          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      "do nothing" when {
        "it is stored and connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          var endpointAdded = false
          module.receive(
            P2PNetworkOut.Admin.AddEndpoint(
              otherInitialEndpointsTupled._1,
              added => endpointAdded = added,
            )
          )

          context.runPipedMessages() shouldBe empty
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs otherInitialEndpoints.map(_ -> None)

          endpointAdded shouldBe false
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }
    }

    "it is requested to remove an endpoint" should {

      "remove and disconnect it" when {
        "it is stored" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, _, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          var endpointRemoved = false
          val removedEndpoint = otherInitialEndpointsTupled._1

          authenticate(p2pNetworkManager, removedEndpoint)

          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              removedEndpoint.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessagesAndReceiveOnModule(module) // Delete endpoint

          val remainingEndpoints =
            Seq(otherInitialEndpointsTupled._2, otherInitialEndpointsTupled._3)
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs remainingEndpoints.map(_ -> None)
          context.extractSelfMessages().foreach(module.receive) // Disconnect endpoint
          endpointRemoved shouldBe true

          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
          verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        }
      }

      "just disconnect the endpoint" when {
        "it is not stored anymore but still connected" in {
          val mempoolSpy = spy(fakeIgnoringModule[Mempool.Message])
          val p2pEndpointsStore = new InMemoryUnitTestP2PEndpointsStore(otherInitialEndpoints.toSet)
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(
              mempool = mempoolSpy,
              p2pEndpointsStore = p2pEndpointsStore,
            )

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          p2pEndpointsStore
            .removeEndpoint(otherInitialEndpointsTupled._1.id)
            .apply() // Endpoint not in store anymore

          var endpointRemoved = false
          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              otherInitialEndpointsTupled._1.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessages() should contain only P2PNetworkOut.Internal.Disconnect(
            otherInitialEndpointsTupled._1.id
          )
          endpointRemoved shouldBe false
          state.p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      "just remove the endpoint" when {
        "it is stored but not connected" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, p2pNetworkManager) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy, connectToInitialNodes = false)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          var endpointRemoved = false
          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              otherInitialEndpointsTupled._1.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessages() shouldBe empty
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs Seq(
            otherInitialEndpointsTupled._2,
            otherInitialEndpointsTupled._3,
          ).map(_ -> None)

          endpointRemoved shouldBe true

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
        }
      }

      "do nothing" when {
        "it is not stored nor connected anymore" in {
          val mempoolSpy =
            spy(fakeIgnoringModule[Mempool.Message])
          val (context, state, module, _) =
            setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

          implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

          var endpointRemoved = false
          module.receive(
            P2PNetworkOut.Admin.RemoveEndpoint(
              anotherEndpoint.id,
              removed => endpointRemoved = removed,
            )
          )

          context.runPipedMessages() shouldBe empty
          module.p2pEndpointsStore
            .listEndpoints()
            .apply() should contain theSameElementsInOrderAs otherInitialEndpoints.map(_ -> None)

          import state.*

          endpointRemoved shouldBe false
          p2pConnectionState.connections should contain theSameElementsAs initialKnownConnections

          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
          verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        }
      }
    }

    "it is queried about configured endpoints" should {
      "return them" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, _, module, _) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        otherInitialEndpoints.foreach(module.p2pEndpointsStore.addEndpoint(_).apply())

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        var endpoints: Option[Seq[(P2PEndpoint, Option[BftNodeId])]] = None
        module.receive(
          P2PNetworkOut.Admin.ListConfiguredEndpoints(e => endpoints = Some(e))
        )

        context.runPipedMessages() shouldBe empty

        endpoints should contain(otherInitialEndpoints.map(_ -> None))
      }
    }

    "it is queried about endpoints status" should {
      "return it" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, _, module, p2pNetworkManager) =
          setupWithIgnoringDefaultDeps(
            mempool = mempoolSpy,
            p2pEndpointsStore = new InMemoryUnitTestP2PEndpointsStore(
              (yetAnotherEndpoint +: otherInitialEndpoints).toSet
            ),
          )

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        // Peer 1 is connected and authenticated
        connect(p2pNetworkManager, otherInitialEndpointsTupled._1)
        authenticate(p2pNetworkManager, otherInitialEndpointsTupled._1)

        // Peer 2 is only connected
        connect(p2pNetworkManager, otherInitialEndpointsTupled._2)

        // Peer 3 is known but disconnected

        // Another peer is unknown

        // Yet another peer was authenticated but got disconnected
        connect(p2pNetworkManager, yetAnotherEndpoint)
        authenticate(p2pNetworkManager, yetAnotherEndpoint)
        disconnect(p2pNetworkManager, yetAnotherEndpoint)

        context.extractSelfMessages().foreach(module.receive) // Simulate authentication

        Table(
          "queried endpoint IDs" -> "expected status",
          Some(
            Seq(
              otherInitialEndpointsTupled._1.id,
              otherInitialEndpointsTupled._2.id,
              anotherEndpoint.id,
            )
          ) -> PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._1.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    endpointToTestSequencerId(otherInitialEndpointsTupled._1)
                  ),
                  None,
                ),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                anotherEndpoint.id,
                isOutgoingConnection = false,
                PeerEndpointHealth(PeerEndpointHealthStatus.UnknownEndpoint, None),
              ),
            )
          ),
          None -> PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._1.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    endpointToTestSequencerId(otherInitialEndpointsTupled._1)
                  ),
                  None,
                ),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._3.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None),
              ),
              PeerConnectionStatus.PeerEndpointStatus(
                yetAnotherEndpoint.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None),
              ),
            )
          ),
        ) forEvery { (queriedEndpoints, expectedStatus) =>
          var status: Option[PeerNetworkStatus] = None
          module.receive(
            P2PNetworkOut.Admin.GetStatus(s => status = Some(s), queriedEndpoints)
          )
          status should contain(expectedStatus)
        }

        disconnect(p2pNetworkManager, otherInitialEndpointsTupled._2)
        context.extractSelfMessages().foreach(module.receive) // Process disconnection

        var status: Option[PeerNetworkStatus] = None
        module.receive(
          P2PNetworkOut.Admin
            .GetStatus(s => status = Some(s), Some(Seq(otherInitialEndpointsTupled._2.id)))
        )
        status should contain(
          PeerNetworkStatus(
            Seq(
              PeerConnectionStatus.PeerEndpointStatus(
                otherInitialEndpointsTupled._2.id,
                isOutgoingConnection = true,
                PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None),
              )
            )
          )
        )

        verify(mempoolSpy, never).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
        verify(mempoolSpy, times(3)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 2))
      }
    }

    "it is sent a topology update" should {
      "send an update to the mempool" in {
        val mempoolSpy =
          spy(fakeIgnoringModule[Mempool.Message])
        val (context, _, module, _) =
          setupWithIgnoringDefaultDeps(mempool = mempoolSpy)

        implicit val ctx: ProgrammableUnitTestContext[P2PNetworkOut.Message] = context

        module.receive(P2PNetworkOut.Network.TopologyUpdate(aMembership))

        verify(mempoolSpy, times(1)).asyncSend(Mempool.P2PConnectivityUpdate(aMembership, 1))
      }
    }
  }

  private def setupWithIgnoringDefaultDeps(
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit = (_, _) => (),
      p2pNetworkIn: ModuleRef[BftOrderingMessage] = fakeModuleExpectingSilence,
      mempool: ModuleRef[Mempool.Message] = fakeIgnoringModule,
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]] = fakeIgnoringModule,
      pruning: ModuleRef[Pruning.Message] = fakeIgnoringModule,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestP2PEndpointsStore(
          otherInitialEndpoints.toSet
        ),
      isGenesis: Boolean = true,
      connectToInitialNodes: Boolean = true,
  ): (
      ProgrammableUnitTestContext[P2PNetworkOut.Message],
      P2PNetworkOutModule.State,
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) =
    setupWithDefaultDepsExpectingSilence(
      mempool,
      availability,
      consensus,
      output,
      pruning,
      sendAction,
      p2pNetworkIn,
      p2pEndpointsStore,
      isGenesis,
      connectToInitialNodes,
    )

  private def setupWithDefaultDepsExpectingSilence(
      mempool: ModuleRef[Mempool.Message],
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]],
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]],
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]],
      pruning: ModuleRef[Pruning.Message],
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit = (_, _) => (),
      p2pNetworkIn: ModuleRef[BftOrderingMessage] = fakeModuleExpectingSilence,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv] =
        new InMemoryUnitTestP2PEndpointsStore(
          otherInitialEndpoints.toSet
        ),
      isGenesis: Boolean = true,
      connectToInitialNodes: Boolean = true,
  ): (
      ProgrammableUnitTestContext[P2PNetworkOut.Message],
      P2PNetworkOutModule.State,
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) = {
    val p2pConnectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
    val state =
      new P2PNetworkOutModule.State(
        p2pConnectionState,
        aMembership,
      )
    implicit val context: ProgrammableUnitTestContext[P2PNetworkOut.Message] =
      new ProgrammableUnitTestContext[P2PNetworkOut.Message](resolveAwaits = true)
    val (module, p2pNetworkManager) =
      createModule(
        sendAction,
        p2pNetworkIn,
        mempool,
        availability,
        consensus,
        output,
        pruning,
        state,
        p2pEndpointsStore,
        isGenesis,
      )
    module.ready(context.self)(TraceContext.createNew("p2p-network-out-module-test"))
    context.selfMessages should contain only P2PNetworkOut.Start
    if (connectToInitialNodes)
      context.extractSelfMessages().foreach(module.receive) // Start connecting to initial nodes
    (context, state, module, p2pNetworkManager)
  }

  private def createModule(
      sendAction: (P2PEndpoint, BftOrderingMessage) => Unit,
      p2pNetworkIn: ModuleRef[BftOrderingMessage],
      mempool: ModuleRef[Mempool.Message],
      availability: ModuleRef[Availability.Message[ProgrammableUnitTestEnv]],
      consensus: ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]],
      output: ModuleRef[Output.Message[ProgrammableUnitTestEnv]],
      pruning: ModuleRef[Pruning.Message],
      state: P2PNetworkOutModule.State,
      p2pEndpointsStore: P2PEndpointsStore[ProgrammableUnitTestEnv],
      isGenesis: Boolean,
  ): (
      P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager],
      FakeP2PNetworkManager,
  ) = {
    val dependencies = P2PNetworkOutModuleDependencies(
      (p2pConnectionEventListener, _) =>
        new FakeP2PNetworkManager(p2pConnectionEventListener, sendAction),
      p2pNetworkIn,
      mempool,
      availability,
      consensus,
      output,
      pruning,
    )
    simClock.reset()
    val module =
      new P2PNetworkOutModule[ProgrammableUnitTestEnv, FakeP2PNetworkManager](
        selfNode,
        isGenesis,
        state,
        new Random(4),
        simClock,
        p2pEndpointsStore,
        SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
        dependencies,
        P2PNetworkOutModuleTest.this.loggerFactory,
        P2PNetworkOutModuleTest.this.timeouts,
      )(MetricsContext.Empty)
    (module, module.p2pNetworkManager)
  }

  private def connect(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
  ): Unit =
    fakeClientP2PNetworkManager
      .nodeActions(endpoint.id)
      .onConnect(Some(endpoint.id))

  private def disconnect(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
  ): Unit =
    fakeClientP2PNetworkManager
      .nodeActions(endpoint.id)
      .onDisconnect(
        endpoint.id
      )

  private def authenticate(
      fakeClientP2PNetworkManager: FakeP2PNetworkManager,
      endpoint: P2PEndpoint,
      customNode: Option[BftNodeId] = None,
  ): Unit = {
    val bftNodeId = customNode.getOrElse(endpointToTestBftNodeId(endpoint))
    fakeClientP2PNetworkManager
      .nodeActions(endpoint.id)
      .onNodeId(bftNodeId, Some(endpoint))
  }

  private class FakeP2PNetworkManager(
      p2pConnectionEventListener: P2PConnectionEventListener,
      asyncP2PSendAction: (P2PEndpoint, BftOrderingMessage) => Unit,
  ) extends P2PNetworkManager[ProgrammableUnitTestEnv, BftOrderingMessage]
      with NamedLogging {

    override val timeouts: ProcessingTimeout = P2PNetworkOutModuleTest.this.timeouts
    override val loggerFactory: NamedLoggerFactory = P2PNetworkOutModuleTest.this.loggerFactory

    val nodeActions: mutable.Map[P2PEndpoint.Id, P2PConnectionEventListener] =
      mutable.Map.empty
    override def createNetworkRef[ActorContextT](
        context: ProgrammableUnitTestContext[ActorContextT],
        p2pAddress: P2PAddress,
    )(implicit traceContext: TraceContext): P2PNetworkRef[BftOrderingMessage] = {
      p2pAddress.maybeP2PEndpoint
        .map(_.id)
        .foreach(nodeActions.put(_, p2pConnectionEventListener).discard)

      new P2PNetworkRef[BftOrderingMessage]() {
        override def asyncP2PSend(
            recipientBftNodeId: BftNodeId,
            createMsg: Option[Instant] => BftOrderingMessage,
        )(implicit
            traceContext: TraceContext,
            metricsContext: MetricsContext,
        ): Unit =
          p2pAddress.maybeP2PEndpoint.foreach(asyncP2PSendAction(_, createMsg(None)))

        override protected def timeouts: ProcessingTimeout =
          P2PNetworkOutModuleTest.this.timeouts

        override protected def logger: TracedLogger = P2PNetworkOutModuleTest.this.logger
      }
    }

    override def shutdownOutgoingConnection(
        p2pEndpointId: P2PEndpoint.Id
    )(implicit traceContext: TraceContext): Unit = ()
  }
}

object P2PNetworkOutModuleTest {

  final class InMemoryUnitTestP2PEndpointsStore(
      initialEndpoints: Set[P2PEndpoint]
  ) extends GenericInMemoryP2PEndpointsStore[ProgrammableUnitTestEnv](initialEndpoints) {
    override protected def createFuture[T](action: String)(
        value: () => Try[T]
    ): ProgrammableUnitTestEnv#FutureUnlessShutdownT[T] = () =>
      value() match {
        case Success(value) => value
        case Failure(exception) => fail(exception)
      }
    override def close(): Unit = ()
  }

  private lazy val selfNode: BftNodeId =
    endpointToTestBftNodeId(PlainTextP2PEndpoint(s"host0", Port.tryCreate(5000)))

  private lazy val otherInitialEndpoints =
    (1 to 3)
      .map(i => PlainTextP2PEndpoint(s"host$i", Port.tryCreate(5000 + i)))

  private lazy val otherInitialEndpointIds =
    otherInitialEndpoints.map(_.id)

  private lazy val initialKnownConnections =
    otherInitialEndpointIds.map(Some(_) -> None)

  private lazy val anotherEndpoint =
    PlainTextP2PEndpoint("host4", Port.tryCreate(5004))

  private lazy val yetAnotherEndpoint =
    PlainTextP2PEndpoint("host5", Port.tryCreate(5005))

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private lazy val otherInitialEndpointsTupled =
    otherInitialEndpoints
      .toHList[P2PEndpoint :: P2PEndpoint :: P2PEndpoint :: HNil]
      .get
      .tupled

  private lazy val bftNodeIds = selfNode +: otherInitialEndpoints.map(endpointToTestBftNodeId)
  private def aMembership(implicit pv: ProtocolVersion) =
    Membership(
      selfNode,
      OrderingTopology.forTesting(bftNodeIds.toSet),
      leaders = bftNodeIds,
      blacklistedNodes = Seq.empty,
    )
}
