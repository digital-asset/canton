// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.concurrent.DirectExecutionContext.directExecutionContext
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.lifecycle.{PromiseUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.{
  OutgoingConnectionAttemptId,
  P2POutgoingConnectionStatus,
  PeerSender,
  State,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.PlainTextP2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  P2PConnectionManagementConfig,
  P2PTransportMetricsConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ManagedChannel
import io.grpc.stub.CallStreamObserver
import org.mockito.invocation.InvocationOnMock
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicReference

class P2PGrpcConnectionManagerTest extends AnyWordSpec with BftSequencerBaseTest {

  private val selfNode = BftNodeId("self")
  private val peerNode = BftNodeId("peer")
  private val endpoint = PlainTextP2PEndpoint("peer", Port.tryCreate(1234))
  private val address = P2PAddress.NodeId(peerNode).id

  "actor-driven connection setup" should {

    "not restore endpoint associations when the ref is removed before the conditional association update" in {
      val realConnectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val connectionState = spy(realConnectionState)
      val manager = newManager(connectionState, mock[P2PConnectionEventListener])
      val oldRef =
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
      val peerAddress = P2PAddress.NodeId(peerNode, Some(endpoint))

      org.mockito.Mockito
        .doAnswer { (_: InvocationOnMock) =>
          // The manager has started processing setup. Remove the ref immediately before the
          //  conditional association update, as concurrent operator endpoint removal could do.
          manager.shutdownConnection(endpoint.id)
          realConnectionState.associateP2PEndpointIdToBftNodeId(peerAddress, oldRef)
        }
        .when(connectionState)
        .associateP2PEndpointIdToBftNodeId(peerAddress, oldRef)

      manager.getPeerSenderOrStartConnection(peerAddress, oldRef) shouldBe None

      // Conditional association must not recreate a mapping without a ref. In particular,
      //  EndpointAdded must see isDefined=false so it can enable sending to this endpoint again.
      connectionState.getBftNodeId(endpoint.id) shouldBe None
      connectionState.getNetworkRef(peerNode) shouldBe None
      connectionState.isDefined(endpoint.id) shouldBe false
      connectionState.connections shouldBe empty
      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(Map.empty)
      verify(oldRef).close()

      val replacementSender = newSender(outgoing = true)
      val replacementRef =
        installPeerConnectionAndAssociateEndpoint(connectionState, replacementSender)
      manager.getPeerSenderOrStartConnection(peerAddress, replacementRef) shouldBe Some(
        replacementSender
      )
      connectionState.getBftNodeId(endpoint.id) shouldBe Some(peerNode)
      verify(replacementRef, never).close()
    }

    "stop an outgoing connection attempt registered between the outgoing connections snapshot and ref removal" in {
      Table(
        ("incoming cleanup", "replace ref"),
        (false, false),
        (true, false),
        (false, true),
        (true, true),
      ).forEvery { (incomingCleanup, replaceRef) =>
        val realConnectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
        val connectionState = spy(realConnectionState)
        val manager = newManager(connectionState, mock[P2PConnectionEventListener])
        val sender = newSender(outgoing = !incomingCleanup)
        val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)
        val outgoingConnectionAttemptId =
          OutgoingConnectionAttemptId(owningNetworkRefO = Some(ref))
        val replacementRef = mock[P2PNetworkRef[BftOrderingMessage]]
        val replacementOutgoingConnectionAttempt = P2POutgoingConnectionStatus.Connecting(
          OutgoingConnectionAttemptId(owningNetworkRefO = Some(replacementRef))
        )
        when(ref.close()).thenAnswer {
          if (replaceRef) {
            // Ref removal has finished, but outgoing cleanup has not. Bind a genuine replacement
            //  outgoing connection attempt to its new ref; the final scan must preserve it.
            connectionState.addNetworkRefIfMissing(address)(() => fail())(() => replacementRef)
            connectionState.associateP2PEndpointIdToBftNodeId(endpoint.id, peerNode) shouldBe Right(
              ()
            )
            manager.stateRef.set(
              State(
                UnlessShutdown.Outcome(Map(endpoint.id -> replacementOutgoingConnectionAttempt))
              )
            )
          }
        }

        def registerOutgoingConnectionAttempt(): Unit = {
          // Teardown has already read its empty outgoing snapshot, but has not removed the ref.
          //  An actor that already passed its ownership check can still register here.
          connectionState.isDefined(endpoint.id) shouldBe true
          manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
            Map.empty
          )
          manager.stateRef.set(
            State(
              UnlessShutdown.Outcome(
                Map(
                  endpoint.id -> P2POutgoingConnectionStatus.Connecting(outgoingConnectionAttemptId)
                )
              )
            )
          )
        }

        // Exercise both teardown entry points. Each reads the outgoing connections snapshot before
        //  calling the state cleanup method intercepted below. The spy inserts registration just
        //  before delegating to real cleanup, deterministically reproducing the race without threads.
        //  Use doAnswer so installing the interception does not itself execute cleanup on the spy.
        if (incomingCleanup) {
          // Remote completion of an incoming stream identifies the connection by its sender.
          org.mockito.Mockito
            .doAnswer { (_: InvocationOnMock) =>
              registerOutgoingConnectionAttempt()
              realConnectionState.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(sender)
            }
            .when(connectionState)
            .shutdownAndCleanupActiveConnectionAndReturnEndpointIds(sender)
          manager.shutdownIncomingConnectionDueToRemoteCompletion(sender)
        } else {
          // Operator endpoint removal identifies the connection by its endpoint and closes its ref.
          org.mockito.Mockito
            .doAnswer { (_: InvocationOnMock) =>
              registerOutgoingConnectionAttempt()
              realConnectionState.shutdownConnectionAndReturnPeerSender(
                Left(endpoint.id),
                clearNetworkRefAssociations = true,
                closeNetworkRef = true,
              )
            }
            .when(connectionState)
            .shutdownConnectionAndReturnPeerSender(
              Left(endpoint.id),
              clearNetworkRefAssociations = true,
              closeNetworkRef = true,
              expectedNetworkRefO = None,
            )
          manager.shutdownConnection(endpoint.id)
        }

        // PostStop cannot help once the ref is removed. Teardown must find and stop the outgoing
        //  connection attempt missed by its original snapshot, before notifying disconnection.
        connectionState.getNetworkRef(peerNode) shouldBe Option.when(replaceRef)(replacementRef)
        manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
          if (replaceRef) Map(endpoint.id -> replacementOutgoingConnectionAttempt) else Map.empty
        )
        val (_, publish) = manager.stateRef
          .get()
          .attemptTransitionToConnectingWithChannel(
            endpoint.id,
            mock[ManagedChannel],
            None,
            Some(outgoingConnectionAttemptId),
          )
        publish.result shouldBe false
      }
    }

    "stop registration by an actor already processing initialization when its ref is removed" in {
      val connectionState = spy(new P2PGrpcConnectionState(selfNode, loggerFactory))
      val manager = newManager(connectionState, mock[P2PConnectionEventListener])
      val ref =
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
      when(connectionState.getSender(Left(endpoint.id))).thenAnswer {
        // Initialize has passed the ref-ownership check. Remove the ref and finish teardown before
        //  returning "no sender", making registration happen after teardown's final outgoing connections scan.
        manager.shutdownConnection(endpoint.id)
        None
      }

      manager.getPeerSenderOrStartConnection(P2PAddress.Endpoint(endpoint), ref) shouldBe None

      // Registration's post-CAS ownership check must stop the orphaned outgoing connection attempt
      //  without creating a real gRPC channel or relying on a subsequent actor message.
      connectionState.connections shouldBe empty
      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
        Map.empty
      )
    }

    "reject channel publication from an outgoing connection attempt replaced before channel creation" in {
      val oldOutgoingConnectionAttemptId = OutgoingConnectionAttemptId()
      val replacementOutgoingConnectionAttempt = P2POutgoingConnectionStatus.Connecting()
      val state = State(
        UnlessShutdown.Outcome(Map(endpoint.id -> replacementOutgoingConnectionAttempt))
      )
      val (unchangedState, publish) = state.attemptTransitionToConnectingWithChannel(
        endpoint.id,
        mock[ManagedChannel],
        None,
        Some(oldOutgoingConnectionAttemptId),
      )
      publish.result shouldBe false
      unchangedState shouldBe state
    }

    "reject sender lookup or connection setup from a removed ref while allowing its genuine replacement" in {
      val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val manager = newManager(connectionState, mock[P2PConnectionEventListener])
      val oldRef =
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
      manager.shutdownConnection(endpoint.id)

      // Simulate the manager call that a queued Initialize/Send would make if processed after ref
      //  removal but before Close. This test calls the manager directly, not through an actor,
      //  so it verifies stale-ref rejection rather than actor mailbox ordering. The call must not
      //  restore endpoint associations or start an outgoing connection attempt.
      manager.getPeerSenderOrStartConnection(
        P2PAddress.NodeId(peerNode, Some(endpoint)),
        oldRef,
      ) shouldBe None
      connectionState.connections shouldBe empty
      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(Map.empty)

      val replacementSender = newSender(outgoing = true)
      val replacementRef =
        installPeerConnectionAndAssociateEndpoint(connectionState, replacementSender)
      manager.getPeerSenderOrStartConnection(P2PAddress.Endpoint(endpoint), oldRef) shouldBe None
      manager.getPeerSenderOrStartConnection(
        P2PAddress.NodeId(peerNode),
        replacementRef,
      ) shouldBe Some(replacementSender)
      connectionState.getSender(address) shouldBe Some(replacementSender)
    }
  }

  "connection shutdown" should {

    "cancel every affected endpoint's worker while preserving replacements" in {

      // Both endpoints resolve to one peer and share a network ref, but each has its own pending
      //  outgoing connection attempt. Removing the first endpoint must stop both outgoing connection
      //  attempts, unless a replacement outgoing connection attempt is installed after ref cleanup.
      Table("replace second endpoint", false, true).forEvery { replaceSecond =>
        val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
        val listener = mock[P2PConnectionEventListener]
        val manager = newManager(connectionState, listener)
        val ref =
          installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
        val secondEndpoint = PlainTextP2PEndpoint("second", Port.tryCreate(1235))
        // Associating the second endpoint with the same peer makes it reuse the installed ref.
        connectionState.associateP2PEndpointIdToBftNodeId(
          secondEndpoint.id,
          peerNode,
        ) shouldBe Right(())

        val firstChannel = mock[ManagedChannel]
        val secondChannel = mock[ManagedChannel]
        val firstWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
        val secondWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
        val firstStatus =
          P2POutgoingConnectionStatus.ConnectingOnChannel(firstChannel, None, Some(firstWorker))
        val secondStatus =
          P2POutgoingConnectionStatus.ConnectingOnChannel(secondChannel, None, Some(secondWorker))
        // Seed pending workers directly to control the race without real channels or scheduling.
        //  Their unresolved futures model workers that have not yet observed a disconnect request.
        manager.stateRef.set(
          State(
            UnlessShutdown.Outcome(
              Map(endpoint.id -> firstStatus, secondEndpoint.id -> secondStatus)
            )
          )
        )
        // A new outgoing connection attempt for the second endpoint, with its own channel,
        //  worker, and attemptId.
        //  Installed during ref.close() when replaceSecond is true; shutdown must preserve it.
        val replacementStatus = P2POutgoingConnectionStatus.ConnectingOnChannel(
          mock[ManagedChannel],
          None,
          Some(PromiseUnlessShutdown.unsupervised[Unit]().futureUS),
        )
        when(ref.close()).thenAnswer {
          // Shutdown removes the shared ref and both endpoint associations before closing the ref.
          //  This callback is the race window before it stops outgoing connection attempts.
          connectionState.isDefined(endpoint.id) shouldBe false
          connectionState.isDefined(secondEndpoint.id) shouldBe false
          // Model the shared actor as belonging to the second endpoint. Its late PostStop cannot
          //  cancel that endpoint's worker: the ownership check rejects the already-removed ref.
          manager.shutdownConnection(
            Left(secondEndpoint.id),
            clearNetworkRefAssociations = true,
            closeNetworkRefs = false,
            expectedNetworkRefO = Some(ref),
          )
          if (replaceSecond) {
            // A concurrent reconnect installs a new ref and outgoing connection attempt for the
            //  second endpoint. Shutdown saved the old outgoing connection attempt IDs and must
            //  preserve this replacement.
            connectionState.addNetworkRefIfMissing(Left(secondEndpoint.id))(() => fail())(() =>
              mock[P2PNetworkRef[BftOrderingMessage]]
            )
            manager.stateRef.set(
              State(
                UnlessShutdown.Outcome(
                  Map(endpoint.id -> firstStatus, secondEndpoint.id -> replacementStatus)
                )
              )
            )
          }
        }

        // Request shutdown through only the first endpoint. It must handle all endpoints affected
        //  by shared-ref cleanup rather than relying on the shared actor to cancel the second.
        manager.shutdownConnection(endpoint.id)

        // Cancellation is cooperative: old workers become DisconnectingFromChannel until their
        //  next retry check. A replacement must retain its ConnectingOnChannel status and identity.
        manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
          Map(
            endpoint.id -> P2POutgoingConnectionStatus.DisconnectingFromChannel(
              firstChannel,
              None,
              firstWorker,
              firstStatus.attemptId,
            ),
            secondEndpoint.id -> (if (replaceSecond) replacementStatus
                                  else
                                    P2POutgoingConnectionStatus.DisconnectingFromChannel(
                                      secondChannel,
                                      None,
                                      secondWorker,
                                      secondStatus.attemptId,
                                    )),
          )
        )
        // Check whether each worker would actually retry. Without a replacement, both endpoint
        //  associations are already gone. If P2PNetworkOutModule processes operator removal of the
        //  second endpoint before Network.Disconnected, isDefined=false makes it skip disconnect.
        //  Thus shutdown above must already have stopped the old worker from retrying; the later
        //  disconnection notification must not be needed to prevent the removed peer reconnecting.
        val (_, firstRetry) =
          manager.stateRef.get().attemptTransitionToRetryConnecting(endpoint.id, firstChannel)
        firstRetry.result shouldBe false
        val (_, secondRetry) = manager.stateRef
          .get()
          .attemptTransitionToRetryConnecting(
            secondEndpoint.id,
            if (replaceSecond) replacementStatus.channel else secondChannel,
          )
        secondRetry.result shouldBe replaceSecond
        // One shared ref is closed, but both endpoints must be reported disconnected so
        //  P2PNetworkOutModule can independently re-arm whichever endpoints remain configured.
        verify(ref).close()
        verify(listener).onDisconnect(eqTo(endpoint.id))(any[TraceContext])
        verify(listener).onDisconnect(eqTo(secondEndpoint.id))(any[TraceContext])
      }
    }

    "stop an outgoing connection attempt saved before channel creation even if it advances during ref cleanup" in {
      val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val manager = newManager(connectionState, mock[P2PConnectionEventListener])
      val ref =
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
      val initialOutgoingConnectionAttempt = P2POutgoingConnectionStatus.Connecting()
      // Prepared upfront, but not registered: cleanup initially captures a pre-channel Connecting.
      val channel = mock[ManagedChannel]
      val worker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
      manager.stateRef.set(
        State(UnlessShutdown.Outcome(Map(endpoint.id -> initialOutgoingConnectionAttempt)))
      )
      when(ref.close()).thenAnswer {
        // Simulate channel creation after the outgoing connection attempt's ID was saved,
        //  before that outgoing connection attempt is stopped.
        val (withChannel, _) = manager.stateRef
          .get()
          .attemptTransitionToConnectingWithChannel(
            endpoint.id,
            channel,
            None,
          )
        val (withWorker, _) = withChannel.attemptTransitionToConnectingWithChannelAndWorker(
          endpoint.id,
          channel,
          worker,
        )
        manager.stateRef.set(withWorker)
      }

      manager.shutdownConnection(endpoint.id)

      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
        Map(
          endpoint.id -> P2POutgoingConnectionStatus.DisconnectingFromChannel(
            channel,
            None,
            worker,
            initialOutgoingConnectionAttempt.attemptId,
          )
        )
      )
    }

    "preserve a replacement installed between ref cleanup and stopping outgoing connection attempts" in {
      Table("has old outgoing channel", false, true).forEvery { hasOldChannel =>
        val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
        val manager = newManager(connectionState, mock[P2PConnectionEventListener])
        val ref =
          installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
        val oldChannel = mock[ManagedChannel]
        val oldWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
        if (hasOldChannel)
          manager.stateRef.set(
            State(
              UnlessShutdown.Outcome(
                Map(
                  endpoint.id -> P2POutgoingConnectionStatus.ConnectingOnChannel(
                    oldChannel,
                    None,
                    Some(oldWorker),
                  )
                )
              )
            )
          )

        val replacementSender = newSender(outgoing = true)
        val replacementChannel = mock[ManagedChannel]
        val replacementWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
        val replacementStatus = P2POutgoingConnectionStatus.ConnectingOnChannel(
          replacementChannel,
          None,
          Some(replacementWorker),
        )
        when(ref.close()).thenAnswer {
          // Ref cleanup has already removed the old ref and endpoint association before calling close.
          connectionState.isDefined(endpoint.id) shouldBe false
          // Install the replacement before shutdown stops the original outgoing connection attempt.
          installPeerConnectionAndAssociateEndpoint(connectionState, replacementSender)
          manager.stateRef.set(
            State(UnlessShutdown.Outcome(Map(endpoint.id -> replacementStatus)))
          )
        }

        // This call cleans up the ref (running the callback above), then tries to stop the outgoing
        //  connection attempt whose ID was saved before cleanup. The attemptId check rejects
        //  the replacement outgoing connection attempt.
        manager.shutdownConnection(
          Left(endpoint.id),
          clearNetworkRefAssociations = true,
          closeNetworkRefs = true,
          expectedNetworkRefO = Some(ref),
        )

        // The stopping step preserved both the replacement ref and its outgoing connection attempt.
        connectionState.getSender(address) shouldBe Some(replacementSender)
        connectionState.isDefined(endpoint.id) shouldBe true
        manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
          Map(endpoint.id -> replacementStatus)
        )
        val (_, retry) =
          manager.stateRef.get().attemptTransitionToRetryConnecting(endpoint.id, replacementChannel)
        retry.result shouldBe true
        verify(ref).close()
        verify(replacementChannel, never).shutdown()
      }
    }

    "stop the outgoing worker whose attemptId was saved before ref cleanup if it is still current" in {
      val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val manager = newManager(connectionState, mock[P2PConnectionEventListener])
      // Register the peer's ref, sender, and endpoint association, plus a pending outgoing worker.
      val ref =
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = true))
      val channel = mock[ManagedChannel]
      val worker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
      manager.stateRef.set(
        State(
          UnlessShutdown.Outcome(
            Map(
              endpoint.id -> P2POutgoingConnectionStatus.ConnectingOnChannel(
                channel,
                None,
                Some(worker),
              )
            )
          )
        )
      )

      // Remember the original outgoing connection attempt's identity for the assertions below.
      //  Unlike the preceding race test, nothing installs a replacement during ref cleanup.
      val outgoingConnectionAttemptId =
        manager.stateRef.get().p2pOutgoingConnectionsStatus.toOption.value(endpoint.id).attemptId
      // Shutdown saves this same attemptId before ref cleanup, then checks it before stopping
      //  the worker. The outgoing connection attempt is still current, so the guard must allow this.
      manager.shutdownConnection(
        Left(endpoint.id),
        clearNetworkRefAssociations = true,
        closeNetworkRefs = true,
        expectedNetworkRefO = Some(ref),
      )

      // Ref cleanup is complete, but stopping the worker is cooperative: its channel and future
      //  remain recorded as disconnecting until the worker observes the request.
      connectionState.connections shouldBe empty
      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
        Map(
          endpoint.id ->
            P2POutgoingConnectionStatus.DisconnectingFromChannel(
              channel,
              None,
              worker,
              outgoingConnectionAttemptId,
            )
        )
      )
      // Simulate the worker's next retry check: it must stop rather than reconnect the removed peer.
      val (_, retry) =
        manager.stateRef.get().attemptTransitionToRetryConnecting(endpoint.id, channel)
      retry.result shouldBe false
    }
  }

  "incoming cleanup" should {

    "stop an outgoing connection attempt initially in Connecting with no registered channel, but preserve replacements" in {
      // These cases describe what happens to the outgoing connection attempt while the incoming
      //  ref closes, after its associations are removed but before outgoing attempts are stopped:
      //  - unchanged: the original outgoing connection attempt still has no registered channel.
      //  - channel created: that same outgoing connection attempt registers a channel and worker.
      //  - replaced: a new connection/ref and a different outgoing connection attempt are installed.
      Table("during ref cleanup", "unchanged Connecting", "channel created", "replaced").forEvery {
        phase =>
          val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
          val manager = newManager(connectionState, mock[P2PConnectionEventListener])
          val sender = newSender(outgoing = false)
          val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)
          val initialOutgoingConnectionAttempt = P2POutgoingConnectionStatus.Connecting()
          val replacementOutgoingConnectionAttempt = P2POutgoingConnectionStatus.Connecting()
          val channel = mock[ManagedChannel]
          val worker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
          // Start with an incoming connection and a simultaneous outgoing connection attempt in
          //  Connecting. The mock channel is prepared above, but is not yet registered in this state.
          manager.stateRef.set(
            State(UnlessShutdown.Outcome(Map(endpoint.id -> initialOutgoingConnectionAttempt)))
          )
          when(ref.close()).thenAnswer {
            // Incoming cleanup has saved the original outgoing connection attempt's ID and removed
            //  the incoming ref. Use close() to deterministically simulate activity in this race window.
            connectionState.isDefined(endpoint.id) shouldBe false
            phase match {
              case "channel created" =>
                // Advance the original outgoing connection attempt without changing its attemptId.
                val (withChannel, _) =
                  manager.stateRef
                    .get()
                    .attemptTransitionToConnectingWithChannel(
                      endpoint.id,
                      channel,
                      None,
                    )
                val (withWorker, _) = withChannel.attemptTransitionToConnectingWithChannelAndWorker(
                  endpoint.id,
                  channel,
                  worker,
                )
                manager.stateRef.set(withWorker)
              case "replaced" =>
                // Install a replacement with a distinct attemptId, which old cleanup must not stop.
                installPeerConnectionAndAssociateEndpoint(
                  connectionState,
                  newSender(outgoing = false),
                )
                manager.stateRef.set(
                  State(
                    UnlessShutdown.Outcome(Map(endpoint.id -> replacementOutgoingConnectionAttempt))
                  )
                )
              case _ => ()
            }
          }

          // Remote completion removes the incoming connection and calls ref.close(), executing the
          //  body registered by when(ref.close()).thenAnswer above. It then stops outgoing connection
          //  attempts only if their saved attemptId still matches.
          manager.shutdownIncomingConnectionDueToRemoteCompletion(sender)

          // An unchanged Connecting entry is removed immediately. An original worker that registered
          //  a channel is asked to disconnect cooperatively. A replacement remains untouched.
          //  Check the outgoing connection state because the ref.close() stub changes those attempts
          //  while incoming cleanup is in progress; removing the incoming ref alone is not enough.
          val expectedStatuses = phase match {
            case "channel created" =>
              Map(
                endpoint.id -> P2POutgoingConnectionStatus.DisconnectingFromChannel(
                  channel,
                  None,
                  worker,
                  initialOutgoingConnectionAttempt.attemptId,
                )
              )
            case "replaced" => Map(endpoint.id -> replacementOutgoingConnectionAttempt)
            case _ => Map.empty
          }
          manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
            expectedStatuses
          )
          if (phase == "unchanged Connecting") {
            // A removed endpoint's stopped outgoing connection attempt must not publish its channel later.
            val (_, publish) = manager.stateRef
              .get()
              .attemptTransitionToConnectingWithChannel(
                endpoint.id,
                channel,
                None,
              )
            publish.result shouldBe false
          }
          // Only the replacement case reintroduces the endpoint association; the old ref closes once.
          connectionState.isDefined(endpoint.id) shouldBe (phase == "replaced")
          verify(ref).close()
      }
    }

    "cancel the pending outgoing worker even when actor teardown no longer owns the ref" in {
      Table("send failure", false, true).forEvery { sendFailure =>
        val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
        val listener = mock[P2PConnectionEventListener]
        val manager = newManager(connectionState, listener)
        val sender = newSender(outgoing = false)
        val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)
        val channel = mock[ManagedChannel]
        val worker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
        manager.stateRef.set(
          State(
            UnlessShutdown.Outcome(
              Map(
                endpoint.id -> P2POutgoingConnectionStatus.ConnectingOnChannel(
                  channel,
                  None,
                  Some(worker),
                )
              )
            )
          )
        )
        when(ref.close()).thenAnswer {
          connectionState.isDefined(endpoint.id) shouldBe false
          // Simulate PostStop after incoming cleanup already removed the actor's ref.
          manager.shutdownConnection(
            Left(endpoint.id),
            clearNetworkRefAssociations = true,
            closeNetworkRefs = false,
            expectedNetworkRefO = Some(ref),
          )
        }

        val outgoingConnectionAttemptId =
          manager.stateRef.get().p2pOutgoingConnectionsStatus.toOption.value(endpoint.id).attemptId
        if (sendFailure)
          manager.handleSendFailure(address, sender, new RuntimeException("send failed"))
        else manager.shutdownIncomingConnectionDueToRemoteCompletion(sender)

        connectionState.connections shouldBe empty
        manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
          Map(
            endpoint.id -> P2POutgoingConnectionStatus.DisconnectingFromChannel(
              channel,
              None,
              worker,
              outgoingConnectionAttemptId,
            )
          )
        )
        // The worker must stop rather than reconnect, even if endpoint removal now skips disconnect
        //  because the ref is gone and the disconnection notification has not been processed yet.
        val (stoppedState, retry) =
          manager.stateRef.get().attemptTransitionToRetryConnecting(endpoint.id, channel)
        retry.result shouldBe false
        stoppedState.p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(Map.empty)
        verify(ref).close()
        verify(listener).onDisconnect(eqTo(endpoint.id))(any[TraceContext])
      }
    }

    "preserve a replacement outgoing worker installed while the old ref closes" in {
      val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val listener = mock[P2PConnectionEventListener]
      val manager = newManager(connectionState, listener)
      val sender = newSender(outgoing = false)
      val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)
      val oldChannel = mock[ManagedChannel]
      val oldWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
      manager.stateRef.set(
        State(
          UnlessShutdown.Outcome(
            Map(
              endpoint.id -> P2POutgoingConnectionStatus.ConnectingOnChannel(
                oldChannel,
                None,
                Some(oldWorker),
              )
            )
          )
        )
      )
      val replacementChannel = mock[ManagedChannel]
      val replacementWorker = PromiseUnlessShutdown.unsupervised[Unit]().futureUS
      val replacementStatus = P2POutgoingConnectionStatus.ConnectingOnChannel(
        replacementChannel,
        None,
        Some(replacementWorker),
      )
      when(ref.close()).thenAnswer {
        installPeerConnectionAndAssociateEndpoint(connectionState, newSender(outgoing = false))
        manager.stateRef.set(
          State(UnlessShutdown.Outcome(Map(endpoint.id -> replacementStatus)))
        )
      }

      manager.shutdownIncomingConnectionDueToRemoteCompletion(sender)
      manager.handleSendFailure(address, sender, new RuntimeException("late send failure"))

      connectionState.isDefined(endpoint.id) shouldBe true
      manager.stateRef.get().p2pOutgoingConnectionsStatus shouldBe UnlessShutdown.Outcome(
        Map(endpoint.id -> replacementStatus)
      )
      val (_, retry) =
        manager.stateRef.get().attemptTransitionToRetryConnecting(endpoint.id, replacementChannel)
      retry.result shouldBe true
      verify(oldChannel, never).shutdown()
      verify(replacementChannel, never).shutdown()
      verify(listener).onDisconnect(eqTo(endpoint.id))(any[TraceContext])
    }
  }

  "send failure cleanup" should {

    "close an incoming ref regardless of remote-completion ordering and preserve its replacement" in {
      Table("remote completion first", false, true).forEvery { remoteCompletionFirst =>
        val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
        val listener = mock[P2PConnectionEventListener]
        val manager = newManager(connectionState, listener)
        val sender = newSender(outgoing = false)
        val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)
        val failure = new RuntimeException("send failed")

        if (remoteCompletionFirst)
          connectionState.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(
            sender
          ) shouldBe Seq(
            endpoint.id
          )

        manager.handleSendFailure(address, sender, failure)
        connectionState.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(
          sender
        ) shouldBe empty
        connectionState.connections shouldBe empty
        connectionState.getNetworkRef(peerNode) shouldBe None
        connectionState.getBftNodeId(endpoint.id) shouldBe None
        connectionState.getSender(address) shouldBe None
        verify(ref, times(1)).close()
        verify(sender.grpcStreamObserver).onError(failure)
        if (!remoteCompletionFirst)
          verify(listener).onDisconnect(eqTo(endpoint.id))(any[TraceContext])
        else verifyNoMoreInteractions(listener)

        val replacementSender = newSender(outgoing = false)
        val replacementRef =
          installPeerConnectionAndAssociateEndpoint(connectionState, replacementSender)
        manager.handleSendFailure(address, sender, failure)
        connectionState.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(
          sender
        ) shouldBe empty
        connectionState.getNetworkRef(peerNode) shouldBe Some(replacementRef)
        connectionState.getSender(address) shouldBe Some(replacementSender)
        connectionState.getBftNodeId(endpoint.id) shouldBe Some(peerNode)
        verify(replacementRef, never).close()
      }
    }

    "retain an outgoing ref and its endpoint association for retries" in {
      val connectionState = new P2PGrpcConnectionState(selfNode, loggerFactory)
      val listener = mock[P2PConnectionEventListener]
      val manager = newManager(connectionState, listener)
      val sender = newSender(outgoing = true)
      val ref = installPeerConnectionAndAssociateEndpoint(connectionState, sender)

      manager.handleSendFailure(address, sender, new RuntimeException("send failed"))

      connectionState.getSender(address) shouldBe None
      connectionState.getNetworkRef(peerNode) shouldBe Some(ref)
      connectionState.getBftNodeId(endpoint.id) shouldBe Some(peerNode)
      verify(ref, never).close()
      verify(listener).onDisconnect(eqTo(endpoint.id))(any[TraceContext])
    }
  }

  private def installPeerConnectionAndAssociateEndpoint(
      connectionState: P2PGrpcConnectionState,
      sender: PeerSender,
  ): P2PNetworkRef[BftOrderingMessage] = {
    val ref = mock[P2PNetworkRef[BftOrderingMessage]]
    connectionState.addNetworkRefIfMissing(address)(() => fail())(() => ref)
    connectionState.associateP2PEndpointIdToBftNodeId(endpoint.id, peerNode) shouldBe Right(())
    connectionState.addSenderIfMissing(peerNode, sender) shouldBe true
    ref
  }

  private def newSender(outgoing: Boolean): PeerSender =
    new PeerSender(
      mock[CallStreamObserver[BftOrderingMessage]],
      isOutgoingConnection = outgoing,
      flowControlEnabled = false,
      maxSendQueueSize = PositiveInt.one,
      dropNewest = false,
      readyAllowance = PositiveInt.one,
      installOnReadyHandlerO = Some(_ => ()),
      onBusyDuration = _ => (),
      onGrpcOnNext = _ => (),
      onAsyncSendError = (_, _) => fail("Unexpected asynchronous send"),
      targetBftNodeIdRef = new AtomicReference(Some(peerNode)),
    )

  private def newManager(
      state: P2PGrpcConnectionState,
      listener: P2PConnectionEventListener,
  ): P2PGrpcConnectionManager =
    new P2PGrpcConnectionManager(
      selfNode,
      P2PConnectionManagementConfig(),
      state,
      mock[P2PEndpointsStore[PekkoEnv]],
      authenticationInitialState = None,
      serverToClientAuthenticationEndpoint = None,
      listener,
      mock[ModuleRef[BftOrderingMessage]],
      mock[BftOrderingMetrics],
      mock[Clock],
      P2PTransportMetricsConfig(enabled = false),
      mock[ExecutorService],
      timeouts,
      loggerFactory,
    )(directExecutionContext, metricsContext)
}
