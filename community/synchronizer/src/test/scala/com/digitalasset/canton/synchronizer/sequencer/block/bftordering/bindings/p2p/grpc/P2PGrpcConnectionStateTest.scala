// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.PeerSender
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import org.mockito.MockitoSugar.mock
import org.scalatest.wordspec.AnyWordSpec

class P2PGrpcConnectionStateTest extends AnyWordSpec with BftSequencerBaseTest {

  import P2PGrpcConnectionStateTest.*

  "P2PGrpcConnectionState" should {

    "associate multiple P2P endpoint ID to a BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId)
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
      state.isConnected(Right(APeerBftNodeId)) shouldBe false

      state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)

      state.connections should contain theSameElementsAs Seq(
        Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
        Some(AnotherPeerP2PEndpoint.id) -> Some(APeerBftNodeId),
      )
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe false
      state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe false
      state.isConnected(Right(APeerBftNodeId)) shouldBe false
    }

    "reject associating a P2P endpoint ID to the self BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      // A self-connection is a misconfiguration rather than a security issue, but it is still
      //  logged as a problem, so that the operator notices and fixes the configuration
      suppressProblemLogs(
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, SelfBftNodeId) shouldBe Left(
          P2PConnectionState.Error
            .CannotAssociateP2PEndpointIdsToSelf(APeerP2PEndpoint.id, SelfBftNodeId)
        )
      )

      state.connections shouldBe empty
      state.isDefined(APeerP2PEndpoint.id) shouldBe false
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
    }

    "reject associating a P2P endpoint ID to the self BFT node ID " +
      "even if it is already associated with a peer BFT node ID" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // E.g., the association was restored from the P2P endpoints store at startup, or the
        //  endpoint was moved to this node after having been associated with a peer.
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId) shouldBe Right(
          ()
        )

        suppressProblemLogs(
          state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, SelfBftNodeId) shouldBe Left(
            P2PConnectionState.Error
              .CannotAssociateP2PEndpointIdsToSelf(APeerP2PEndpoint.id, SelfBftNodeId)
          )
        )

        // The endpoint must not be re-associated to this node; the existing association is left
        //  untouched, so that the ensuing connection shutdown can clean up the peer's connection
        //  state consistently by node ID.
        state.getBftNodeId(APeerP2PEndpoint.id) shouldBe Some(APeerBftNodeId)
        state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId)
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
      }

    // TODO(#34191): re-enable and complete coverage after fixing
    "reject associating a P2P endpoint ID to a BFT node ID if already associated to another BFT node ID" ignore {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.connections shouldBe empty

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      suppressProblemLogs(
        state.associateP2PEndpointIdToBftNodeId(
          APeerP2PEndpoint.id,
          AnotherPeerBftNodeId,
        ) shouldBe Left(
          P2PConnectionState.Error
            .P2PEndpointIdAlreadyAssociated(
              APeerP2PEndpoint.id,
              APeerBftNodeId,
              AnotherPeerBftNodeId,
            )
        )
      )

      state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId)
      state.isDefined(APeerP2PEndpoint.id) shouldBe true
      state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
      state.isConnected(Right(APeerBftNodeId)) shouldBe false
    }

    "associate a gRPC streaming sender with a BFT node ID only if missing" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.getSender(APeerP2PNodeAddressId) shouldBe None

      state.isConnected(APeerP2PNodeAddressId) shouldBe false

      state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true

      state.getSender(APeerP2PNodeAddressId) shouldBe Some(ASender)

      state.isConnected(APeerP2PNodeAddressId) shouldBe true

      state.addSenderIfMissing(APeerBftNodeId, AnotherSender) shouldBe false

      state.getSender(APeerP2PNodeAddressId) shouldBe Some(ASender)

      state.isConnected(APeerP2PNodeAddressId) shouldBe true
    }

    "associate a network ref with an address ID only if missing" in {
      Table("address ID", APeerP2PNodeAddressId, APeerP2PEndpointAddressId).forEvery { addressId =>
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        def checkP2PAddressState(
            isEndpointPresent: Boolean,
            maybeNetworkRef: Option[P2PNetworkRef[BftOrderingMessage]],
        ) =
          addressId match {
            case Left(endpoint) =>
              state.isDefined(endpoint) shouldBe isEndpointPresent
              state.isOutgoing(endpoint) shouldBe isEndpointPresent
            case Right(nodeId) =>
              state.getNetworkRef(nodeId) shouldBe maybeNetworkRef
          }

        var alreadyPresent = false
        def raiseAlreadyPresent(): Unit =
          alreadyPresent = true
        var maybeRef1: Option[P2PNetworkRef[BftOrderingMessage]] = None
        var maybeRef2: Option[P2PNetworkRef[BftOrderingMessage]] = None

        checkP2PAddressState(isEndpointPresent = false, None)

        state.addNetworkRefIfMissing(addressId) { () =>
          raiseAlreadyPresent()
        } { () =>
          val ref = newNetworkRef()
          maybeRef1 = Some(ref)
          ref
        }

        alreadyPresent shouldBe false
        maybeRef1 should not be None
        checkP2PAddressState(isEndpointPresent = true, maybeRef1)
        state.isConnected(addressId) shouldBe false

        state.addNetworkRefIfMissing(addressId) { () =>
          raiseAlreadyPresent()
        } { () =>
          val ref = newNetworkRef()
          maybeRef2 = Some(ref)
          ref
        }

        alreadyPresent shouldBe true
        maybeRef2 shouldBe None
        checkP2PAddressState(isEndpointPresent = true, maybeRef1)
        state.isConnected(addressId) shouldBe false
      }
    }

    "consolidate network refs correctly" when {

      "only an incoming connection exists" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Incoming connection
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

        state.connections should contain only None -> Some(APeerBftNodeId)

        // Unrelated connection
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

        // Outgoing connection authenticates and associates with the same node ID as the incoming connection
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        verifyZeroInteractions(ref1)

        verifyZeroInteractions(ref3)

        state.connections should contain theSameElementsAs Seq(
          Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
          Some(AnotherPeerP2PEndpoint.id) -> None,
        )

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
        state.isDefined(APeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(APeerBftNodeId)) shouldBe false

        // Unrelated connection is unaffected
        state.getNetworkRef(AnotherPeerBftNodeId) shouldBe None
        state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(APeerBftNodeId)) shouldBe false
      }

      "only an outgoing connection exists" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Outgoing connection to unknown node ID
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref1)

        state.connections should contain only Some(APeerP2PEndpoint.id) -> None

        // Unrelated connection
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

        // Outgoing connection authenticates and associates with the same node ID as the incoming connection
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        verifyZeroInteractions(ref1)

        verifyZeroInteractions(ref3)

        state.connections should contain theSameElementsAs Seq(
          Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
          Some(AnotherPeerP2PEndpoint.id) -> None,
        )

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
        state.isDefined(APeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe true
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(APeerBftNodeId)) shouldBe false

        // Unrelated connection is unaffected
        state.getNetworkRef(AnotherPeerBftNodeId) shouldBe None
        state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(AnotherPeerBftNodeId)) shouldBe false
      }

      "an incoming connection exists first" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Incoming connection
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

        state.connections should contain only None -> Some(APeerBftNodeId)

        // Outgoing connection to unknown node ID
        val ref2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref2)

        // Unrelated connection
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

        // Outgoing connection authenticates and associates with the same node ID as the incoming connection
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        verifyZeroInteractions(ref1)
        verify(ref2, times(1)).close()

        verifyZeroInteractions(ref3)

        state.connections should contain theSameElementsAs Seq(
          Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
          Some(AnotherPeerP2PEndpoint.id) -> None,
        )

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
        state.isDefined(APeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(APeerBftNodeId)) shouldBe false

        // Unrelated connection is unaffected
        state.getNetworkRef(AnotherPeerBftNodeId) shouldBe None
        state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(AnotherPeerBftNodeId)) shouldBe false
      }

      "an outgoing connection exists first" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Outgoing connection to unknown node ID
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref1)

        state.connections should contain only Some(APeerP2PEndpoint.id) -> None

        // Incoming connection
        val ref2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref2)

        // Unrelated connection
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

        // Outgoing connection authenticates and associates with the same node ID as the incoming connection
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        verifyZeroInteractions(ref2)
        verify(ref1, times(1)).close()

        verifyZeroInteractions(ref3)

        state.connections should contain theSameElementsAs Seq(
          Some(APeerP2PEndpoint.id) -> Some(APeerBftNodeId),
          Some(AnotherPeerP2PEndpoint.id) -> None,
        )

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref2)
        state.isDefined(APeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(APeerBftNodeId)) shouldBe false

        // Unrelated connection is unaffected
        state.getNetworkRef(AnotherPeerBftNodeId) shouldBe None
        state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe true
        state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Right(AnotherPeerBftNodeId)) shouldBe false
      }
    }

    "shutting down a connection" should {
      "remove the connection and close the network ref" in {
        Table[P2PAddress.Id, Map[P2PEndpoint.Id, BftNodeId], Boolean, Boolean](
          (
            "address ID",
            "endpoint associations",
            "clean network ref associations",
            "close network refs",
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            false,
            false,
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            true,
            false,
          ),
          (
            APeerP2PNodeAddressId,
            Map.empty,
            true,
            true,
          ),
          (
            APeerP2PEndpointAddressId,
            Map(APeerP2PEndpoint.id -> APeerBftNodeId),
            false,
            false,
          ),
          (
            APeerP2PEndpointAddressId,
            Map(APeerP2PEndpoint.id -> APeerBftNodeId),
            true,
            false,
          ),
          (APeerP2PEndpointAddressId, Map(APeerP2PEndpoint.id -> APeerBftNodeId), true, true),
          (AnotherPeerP2PEndpointAddressId, Map.empty, false, false),
          (AnotherPeerP2PEndpointAddressId, Map.empty, true, false),
          (AnotherPeerP2PEndpointAddressId, Map.empty, true, true),
        ).forEvery {
          (
              addressId,
              endpointToBftNodeIdAssociations,
              clearNetworkRefAssociations,
              closeNetworkRefs,
          ) =>
            val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)
            endpointToBftNodeIdAssociations.foreach { case (endpointId, bftNodeId) =>
              state.associateP2PEndpointIdToBftNodeId(endpointId, bftNodeId)
            }
            val ref = newNetworkRef()
            state.addNetworkRefIfMissing(addressId)(() => fail())(() => ref)
            addressId match {
              case Right(nodeId) =>
                state.addSenderIfMissing(nodeId, ASender).discard
              case Left(_) => ()
            }

            val (peerSenderO, affectedP2PEndpointIds) =
              state.shutdownConnectionAndReturnPeerSender(
                addressId,
                clearNetworkRefAssociations,
                closeNetworkRefs,
              )

            state.getSender(addressId) shouldBe None
            state.isConnected(addressId) shouldBe false

            addressId match {
              case Right(nodeId) =>
                peerSenderO should not be None
                // All endpoints associated with the node are reported as affected by the shutdown
                val associatedEndpointIds =
                  endpointToBftNodeIdAssociations.collect { case (endpointId, `nodeId`) =>
                    endpointId
                  }
                affectedP2PEndpointIds should contain theSameElementsAs associatedEndpointIds

                state.getNetworkRef(nodeId) shouldBe (if (clearNetworkRefAssociations) None
                                                      else Some(ref))
                // When associations are cleared, the endpoint-to-node mappings for this node
                //  are also cleared, so a subsequent admin re-add can reconnect the endpoints.
                associatedEndpointIds.foreach { endpointId =>
                  state.isDefined(endpointId) shouldBe !clearNetworkRefAssociations
                }
              case Left(endpointId) =>
                peerSenderO shouldBe None
                // The requesting endpoint is always reported as affected, so that the earlier
                //  `onConnect` is properly balanced by an `onDisconnect` notification.
                affectedP2PEndpointIds should contain(endpointId)
                // Clearing associations clears both the endpoint's network ref entry and,
                //  transitively via the node it was associated with, its endpoint-to-node mapping,
                //  so `isDefined` reflects the shutdown; otherwise nothing is cleared.
                state.isDefined(endpointId) shouldBe !clearNetworkRefAssociations
                // The connection was outgoing (created via a `Left` address ID),
                //  so `isOutgoing` is true as long as the network ref entry is still
                //  present, i.e., when associations were not cleared.
                state.isOutgoing(endpointId) shouldBe !clearNetworkRefAssociations
            }

            if (closeNetworkRefs)
              verify(ref, times(1)).close()
            else
              verifyZeroInteractions(ref)
        }
      }
    }

    "shutting down and cleaning up an active (incoming) connection by sender" should {

      "remove the sender, close the network ref and clear its associations" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Incoming connection: a network ref, a sender and an endpoint ID are associated with the BFT node ID
        val ref = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref)
        state.addSenderIfMissing(APeerBftNodeId, ASender).discard
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        state.isConnected(APeerP2PNodeAddressId) shouldBe true
        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref)

        state.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(ASender)

        // The sender is removed and the connection is no longer active
        state.getSender(APeerP2PNodeAddressId) shouldBe None
        state.isConnected(APeerP2PNodeAddressId) shouldBe false

        // The network ref is closed and association is cleared
        verify(ref, times(1)).close()
        state.getNetworkRef(APeerBftNodeId) shouldBe None
        state.getBftNodeId(APeerP2PEndpoint.id) shouldBe None
        state.connections shouldBe empty
      }

      "be a no-op for an unknown sender" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        state.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(ASender)

        state.connections shouldBe empty
      }
    }

    "retrying an outgoing connection after cleanup" should {
      "correctly report `isOutgoing` after the endpoint was already associated with a BFT node ID" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Outgoing connection
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref1)

        // Incoming connection
        val ref2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref2)

        // Outgoing connection authenticates and associates with the same node ID as the incoming connection
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        // Incoming connection wins, outgoing is closed
        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref2)
        verify(ref1, times(1)).close()
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false

        // Simulate cleanup (both connections die and network ref associations are cleared)
        state.shutdownConnectionAndReturnPeerSender(
          APeerP2PNodeAddressId,
          clearNetworkRefAssociations = true,
          closeNetworkRef = true,
        )
        verify(ref2, times(1)).close()
        state.getNetworkRef(APeerBftNodeId) shouldBe None
        // The endpoint-to-node mapping was also cleared, so a subsequent retry re-authenticates
        //  before the ref can be routed to the node.
        state.getBftNodeId(APeerP2PEndpoint.id) shouldBe None

        // Retry: new outgoing connection via the same endpoint; the ref is keyed by the endpoint
        //  until authentication re-establishes the endpoint-to-node association.
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref3)

        state.getNetworkRef(APeerBftNodeId) shouldBe None
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe true

        // Re-authentication consolidates the ref onto the node ID.
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref3)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe true
      }
    }

    "retrying an incoming connection after cleanup" should {
      "correctly report `isOutgoing` as false" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Incoming connection
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

        // Associate endpoint
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false

        // Cleanup
        state.shutdownConnectionAndReturnPeerSender(
          APeerP2PNodeAddressId,
          clearNetworkRefAssociations = true,
          closeNetworkRef = true,
        )
        verify(ref1, times(1)).close()
        state.getNetworkRef(APeerBftNodeId) shouldBe None

        // Retry: new incoming connection
        val ref2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref2)

        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref2)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
      }
    }

    "handle an incoming connection, endpoint removal, reconnection, shutdown, " +
      "and a different node reusing the same endpoint" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // node1 has an endpoint entry for node2 (operator-configured, not yet connected)
        // Modeled as: an outgoing network ref is added for the endpoint.
        val outgoingRefToNode2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() =>
          outgoingRefToNode2
        )
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe true
        state.getNetworkRef(APeerBftNodeId) shouldBe None

        // node2 connects first to node1 (incoming) and authenticates as node2's BFT node ID.
        val incomingRefFromNode2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() =>
          incomingRefFromNode2
        )
        // Authentication associates node2's endpoint (known to node1) to node2's BFT node ID:
        // the incoming ref wins, the outgoing ref is closed.
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
        verify(outgoingRefToNode2, times(1)).close()
        state.getNetworkRef(APeerBftNodeId) shouldBe Some(incomingRefFromNode2)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        // Sender is registered for the incoming connection.
        state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true
        state.isConnected(APeerP2PNodeAddressId) shouldBe true

        // The operator of node1 removes the endpoint of node2.
        // The connection is still active (incoming), but its outgoing endpoint entry is dropped.
        // Modeled as shutting down via the endpoint address ID, clearing associations.
        state.shutdownConnectionAndReturnPeerSender(
          APeerP2PEndpointAddressId,
          clearNetworkRefAssociations = true,
          closeNetworkRef = false,
        )

        // The incoming connection (by node ID) is also gone.
        state.getNetworkRef(APeerBftNodeId) shouldBe None
        state.isConnected(APeerP2PNodeAddressId) shouldBe false
        // Since associations were cleared on shutdown, the endpoint-to-node mapping is gone too,
        //  so the endpoint is no longer known (consistently with removing the endpoint).
        state.isDefined(APeerP2PEndpoint.id) shouldBe false

        // node2 reconnects to node1 (incoming, same node ID).
        val incomingRefFromNode2b = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail()) { () =>
          incomingRefFromNode2b
        }
        // Authentication associates node2's endpoint (known to node1) to node2's BFT node ID:
        // the incoming ref wins, the outgoing ref is closed.
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
        state.getNetworkRef(APeerBftNodeId) shouldBe Some(incomingRefFromNode2b)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        // Sender is registered for the incoming connection.
        state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true
        state.isConnected(APeerP2PNodeAddressId) shouldBe true

        // node2 is then shut down, which causes sender-driven cleanup on node1.
        state.shutdownAndCleanupActiveConnectionAndReturnEndpointIds(ASender)
        verify(incomingRefFromNode2b, times(1)).close()
        state.getSender(APeerP2PNodeAddressId) shouldBe None
        state.isConnected(APeerP2PNodeAddressId) shouldBe false
        state.getNetworkRef(APeerBftNodeId) shouldBe None
        // No lingering peer senders nor network refs for node2, and no endpoint entry.
        state.connections shouldBe empty
        state.isDefined(APeerP2PEndpoint.id) shouldBe false

        // node3 connects to node1 (incoming) and communicates the same endpoint of node2
        // but with a different BFT node ID.
        val incomingRefFromNode3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PNodeAddressId)(() => fail())(() =>
          incomingRefFromNode3
        )
        // Authentication associates the reused endpoint to node3's BFT node ID.
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, AnotherPeerBftNodeId)
        state.addSenderIfMissing(AnotherPeerBftNodeId, AnotherSender) shouldBe true

        // node1 and node3 are connected.
        state.getNetworkRef(AnotherPeerBftNodeId) shouldBe Some(incomingRefFromNode3)
        state.getSender(AnotherPeerP2PNodeAddressId) shouldBe Some(AnotherSender)
        state.isConnected(AnotherPeerP2PNodeAddressId) shouldBe true
        state.getBftNodeId(APeerP2PEndpoint.id) shouldBe Some(AnotherPeerBftNodeId)

        // node1 has no peer senders nor network refs to node2 anymore.
        state.getNetworkRef(APeerBftNodeId) shouldBe None
        state.getSender(APeerP2PNodeAddressId) shouldBe None
        state.isConnected(APeerP2PNodeAddressId) shouldBe false
        // The only remaining connection entry maps the (previously node2's) endpoint to node3.
        state.connections should contain only Some(APeerP2PEndpoint.id) -> Some(
          AnotherPeerBftNodeId
        )
      }

    "consolidate network refs with multiple endpoints for the same node" should {
      "propagate the winning network ref to all associated endpoints" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        // Incoming connection
        val ref1 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

        // Outgoing connections to two endpoints
        val ref2 = newNetworkRef()
        state.addNetworkRefIfMissing(APeerP2PEndpointAddressId)(() => fail())(() => ref2)
        val ref3 = newNetworkRef()
        state.addNetworkRefIfMissing(AnotherPeerP2PEndpointAddressId)(() => fail())(() => ref3)

        // Both endpoints authenticate as the same node ID
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
        state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)

        // Incoming connection wins for both endpoints
        state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
        verify(ref2, times(1)).close()
        verify(ref3, times(1)).close()

        // Both endpoints report isOutgoing = false (incoming connection won)
        state.isOutgoing(APeerP2PEndpoint.id) shouldBe false
        state.isOutgoing(AnotherPeerP2PEndpoint.id) shouldBe false

        state.getBftNodeId(APeerP2PEndpoint.id) shouldBe Some(APeerBftNodeId)
        state.getBftNodeId(AnotherPeerP2PEndpoint.id) shouldBe Some(APeerBftNodeId)
      }
    }

    "return the BFT node ID for an associated endpoint" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      state.getBftNodeId(APeerP2PEndpoint.id) shouldBe None

      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      state.getBftNodeId(APeerP2PEndpoint.id) shouldBe Some(APeerBftNodeId)
      state.getBftNodeId(AnotherPeerP2PEndpoint.id) shouldBe None
    }

    "unassociating a sender" should {
      "remove the sender and return the endpoints associated with the node ID" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)
        state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
        state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)

        state.addSenderIfMissing(APeerBftNodeId, ASender) shouldBe true
        state.isConnected(APeerP2PNodeAddressId) shouldBe true
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe true
        state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe true

        state.unassociateSenderAndReturnEndpointIds(ASender) should contain theSameElementsAs Seq(
          APeerP2PEndpoint.id,
          AnotherPeerP2PEndpoint.id,
        )
        state.isConnected(APeerP2PNodeAddressId) shouldBe false
        state.isConnected(Left(APeerP2PEndpoint.id)) shouldBe false
        state.isConnected(Left(AnotherPeerP2PEndpoint.id)) shouldBe false
      }

      "return empty for an unknown sender" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        state.unassociateSenderAndReturnEndpointIds(ASender) shouldBe empty
      }
    }

    "shutting down an unknown endpoint" should {
      "be a no-op" in {
        val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

        val (peerSenderO, affectedP2PEndpointIds) =
          state.shutdownConnectionAndReturnPeerSender(
            AnotherPeerP2PEndpointAddressId,
            clearNetworkRefAssociations = true,
            closeNetworkRef = false,
          )

        peerSenderO shouldBe None
        // The requesting endpoint is always reported as affected, so that any earlier `onConnect`
        //  is properly balanced even when the endpoint was never associated.
        affectedP2PEndpointIds shouldBe Seq(AnotherPeerP2PEndpoint.id)
        state.connections shouldBe empty
      }
    }

    "shutting down an endpoint sharing a sender with other endpoints" should {
      "report every endpoint associated with the underlying peer as affected " +
        "and clear their endpoint-to-node mappings" in {
          val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

          // Two endpoints resolving to the same peer, both associated with its BFT node ID.
          state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)
          state.associateP2PEndpointIdToBftNodeId(AnotherPeerP2PEndpoint.id, APeerBftNodeId)
          val ref = newNetworkRef()
          state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref)
          state.addSenderIfMissing(APeerBftNodeId, ASender).discard

          // Shutting down via any single endpoint tears down the shared sender/ref and must report
          //  every endpoint that shared it, so that `onDisconnect` can be notified for each of them.
          val (peerSenderO, affectedP2PEndpointIds) =
            state.shutdownConnectionAndReturnPeerSender(
              Left(APeerP2PEndpoint.id),
              clearNetworkRefAssociations = true,
              closeNetworkRef = true,
            )

          peerSenderO shouldBe Some(ASender)
          affectedP2PEndpointIds should contain theSameElementsAs Seq(
            APeerP2PEndpoint.id,
            AnotherPeerP2PEndpoint.id,
          )
          verify(ref, times(1)).close()
          state.getNetworkRef(APeerBftNodeId) shouldBe None
          // The endpoint-to-node mappings must be cleared too, so a subsequent admin re-add of any
          //  of these endpoints is not skipped due to a stale association.
          state.getBftNodeId(APeerP2PEndpoint.id) shouldBe None
          state.getBftNodeId(AnotherPeerP2PEndpoint.id) shouldBe None
          state.isDefined(APeerP2PEndpoint.id) shouldBe false
          state.isDefined(AnotherPeerP2PEndpoint.id) shouldBe false
        }
    }

    "not create a duplicate network ref via endpoint when one already exists for the BFT node ID" in {
      val state = new P2PGrpcConnectionState(SelfBftNodeId, loggerFactory)

      // Associate endpoint to node ID first
      state.associateP2PEndpointIdToBftNodeId(APeerP2PEndpoint.id, APeerBftNodeId)

      // Create a network ref via the node ID (incoming)
      val ref1 = newNetworkRef()
      state.addNetworkRefIfMissing(APeerP2PNodeAddressId)(() => fail())(() => ref1)

      state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)

      // Try to create a network ref via the endpoint (outgoing), but one already exists for the node
      var alreadyPresent = false
      state.addNetworkRefIfMissing(APeerP2PEndpointAddressId) { () =>
        alreadyPresent = true
      } { () =>
        fail("should not create a new network ref")
      }

      alreadyPresent shouldBe true
      // The existing (incoming) ref is preserved
      state.getNetworkRef(APeerBftNodeId) shouldBe Some(ref1)
    }
  }
}

object P2PGrpcConnectionStateTest {

  private val SelfBftNodeId = BftNodeId("1")

  private val APeerP2PEndpoint = PlainTextP2PEndpoint("1", Port.tryCreate(1))
  private val AnotherPeerP2PEndpoint = PlainTextP2PEndpoint("2", Port.tryCreate(2))

  private val APeerP2PEndpointAddressId = P2PAddress.Endpoint(APeerP2PEndpoint).id
  private val AnotherPeerP2PEndpointAddressId = P2PAddress.Endpoint(AnotherPeerP2PEndpoint).id

  private val APeerBftNodeId = BftNodeId("2")
  private val AnotherPeerBftNodeId = BftNodeId("3")
  private val APeerP2PNodeAddressId = P2PAddress.NodeId(APeerBftNodeId).id
  private val AnotherPeerP2PNodeAddressId = P2PAddress.NodeId(AnotherPeerBftNodeId).id

  private val ASender = mock[PeerSender]
  private val AnotherSender = mock[PeerSender]

  private def newNetworkRef() = mock[P2PNetworkRef[BftOrderingMessage]]
}
