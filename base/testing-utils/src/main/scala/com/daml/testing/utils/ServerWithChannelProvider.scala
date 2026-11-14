// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.utils

import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

final case class ServerWithChannelProvider(server: Server, channel: () => ManagedChannel) {

  def getClient[Stub](createStub: Channel => Stub): Stub = createStub(channel())
}

object ServerWithChannelProvider {
  def fromServices(
      services: Iterable[BindableService],
      address: Option[SocketAddress],
      serverName: String,
  ): ServerWithChannelProvider = {
    val serverBuilder = address.fold[ServerBuilder[? <: ServerBuilder[?]]](
      services.foldLeft(InProcessServerBuilder.forName(serverName))(_ addService _)
    )(a => services.foldLeft(NettyServerBuilder.forAddress(a))(_ addService _))
    val server = serverBuilder
      .build()

    server.start()

    ServerWithChannelProvider(
      server,
      () => getChannel(address.map(_ => server.getPort()), serverName),
    )
  }

  /** Shuts the server down gracefully, so that calls still in flight (e.g. a stream the client has
    * just cancelled, whose remaining responses the server is still flushing) end before the
    * connection is torn down. Only falls back to a forceful shutdown if that takes too long.
    *
    * Aborting a stream while responses are still queued for it makes grpc-netty log
    * `NettyServerHandler onStreamError` "Stream closed before write could take place" at WARNING
    * through java.util.logging, which fails CI's sbt output check. Call this while the clients'
    * channels are still open: closing them first aborts the streams in the same way.
    */
  def shutdownServer(server: Server): Unit = {
    server.shutdown()
    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    }
    ()
  }

  private def getChannel(port: Option[Int], serverName: String) =
    port
      .fold[ManagedChannelBuilder[?]](
        InProcessChannelBuilder
          .forName(serverName)
          .usePlaintext()
      )(p =>
        ManagedChannelBuilder
          .forAddress("127.0.0.1", p)
          .usePlaintext()
      )
      .build()
}
