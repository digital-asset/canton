// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext

trait P2PConnectionState {

  import P2PConnectionState.*

  def isDefined(p2pEndpointId: P2PEndpoint.Id)(implicit traceContext: TraceContext): Boolean

  def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean

  def isConnected(p2pAddressId: P2PAddress.Id)(implicit traceContext: TraceContext): Boolean

  def associateP2PEndpointIdToBftNodeId(
      p2pAddress: P2PAddress
  )(implicit traceContext: TraceContext): P2PEndpointIdAssociationResult[Unit]

  /** Called by the P2P network output module to ensure connectivity with a peer. It must call
    * either `createNetworkRef` to create a new network reference or `actionIfPresent` if a network
    * reference already exists for the given P2P address ID.
    *
    * `createNetworkRef` is invoked synchronously, on the calling thread, and eagerly, i.e. the
    * network reference is fully created before the entry holding it is published into the
    * connection state. This is both a threading contract and a safety property:
    *
    *   - Threading: the network reference is the only entry point through which the connection
    *     actor is spawned, and spawning uses the caller's actor context, which is thread-unsafe and
    *     thus bound to the calling actor's thread. Callers must therefore invoke this method (and
    *     pass a `createNetworkRef` capturing their actor context) only from that actor's thread,
    * i.e. the P2P network output module actor thread.
    *   - Safety: because the reference is created before it becomes visible in the (concurrently
    *     accessed) connection state, a concurrent shutdown or consolidation always observes it and
    *     can close it, rather than racing with a still-pending creation and leaking an untracked,
    *     unclosable connection actor. As a corollary, `createNetworkRef` may be invoked more than
    *     once under contention (once per attempt that observes a missing reference), but at most
    *     one resulting reference is installed and any other is closed immediately.
    *
    * To make speculative creation safe, references use a two-phase start: they are created parked
    * and inert (the connection actor neither initializes nor touches any address-based connection
    * state), and only the single installed reference is activated, after publication, via
    * `startConnection()` (itself a no-op should a concurrent shutdown have closed that reference in
    * the meantime). Any superseded reference is therefore closed without ever having affected the
    * connection state for its address.
    *
    * The network reference is never (re)created lazily by other accessors, such as
    * [[getNetworkRef]] or connection shutdown, which only ever read an already-created reference.
    */
  def addNetworkRefIfMissing(
      p2pAddressId: P2PAddress.Id
  )(
      actionIfPresent: () => Unit
  )(
      createNetworkRef: () => P2PNetworkRef[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit

  def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId]

  /** Returns the network reference for the given BFT node ID if one has already been created,
    * without ever creating (spawning) it. Safe to call from any thread.
    */
  def getNetworkRef(bftNodeId: BftNodeId): Option[P2PNetworkRef[BftOrderingMessage]]

  def connections(implicit
      traceContext: TraceContext
  ): Seq[(Option[P2PEndpoint.Id], Option[BftNodeId])]
}

object P2PConnectionState {

  type P2PEndpointIdAssociationResult[T] = Either[Error, T]

  sealed trait Error extends Product with Serializable

  object Error {

    final case class CannotAssociateP2PEndpointIdsToSelf(
        p2pEndpointId: P2PEndpoint.Id,
        thisBftNodeId: BftNodeId,
    ) extends Error {
      override def toString: String =
        s"Cannot associate any P2P endpoint ID $p2pEndpointId to self ($thisBftNodeId)"
    }

    final case class P2PEndpointIdAlreadyAssociated(
        p2pEndpointId: P2PEndpoint.Id,
        previousBftNodeId: BftNodeId,
        newBftNodeId: BftNodeId,
    ) extends Error {
      override def toString: String =
        s"Cannot associate P2P endpoint ID $p2pEndpointId to $newBftNodeId " +
          s"as it is already associated with $previousBftNodeId"
    }
  }
}
