// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import io.grpc.{Context, Contexts, Grpc, Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import java.net.SocketAddress

/** Captures the TCP remote (peer) socket address of an incoming P2P gRPC call into a gRPC
  * [[io.grpc.Context]] key, so that the transport metrics publisher can correlate the peer's
  * authenticated node ID with the corresponding server socket reported by gRPC Channelz (whose
  * `remote` address is this same value).
  */
private[bftordering] final class P2PTransportMetricsServerInterceptor extends ServerInterceptor {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val remoteAddressO = Option(call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
    val context =
      Context
        .current()
        .withValue(P2PTransportMetricsServerInterceptor.remoteAddressContextKey, remoteAddressO)
    Contexts.interceptCall(context, call, headers, next)
  }
}

private[bftordering] object P2PTransportMetricsServerInterceptor {

  /** Set by [[P2PTransportMetricsServerInterceptor]]; read on the incoming-connection path once the
    * peer is authenticated. Absent (`null`) when the interceptor is not installed.
    */
  val remoteAddressContextKey: Context.Key[Option[SocketAddress]] =
    Context.key("bft-p2p-transport-remote-address")
}
