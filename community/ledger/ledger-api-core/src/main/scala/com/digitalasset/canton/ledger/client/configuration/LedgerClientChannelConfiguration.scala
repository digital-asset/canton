// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.configuration

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import io.grpc.internal.GrpcUtil
import io.grpc.netty.shaded.io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

/** @param sslContext
  *   If defined, the context will be passed on to the underlying gRPC code to ensure the
  *   communication channel is secured by TLS
  * @param maxInboundMetadataSize
  *   The maximum size of the response headers.
  * @param maxInboundMessageSize
  *   The maximum (uncompressed) size of the response body.
  * @param flowControlWindow
  *   Switches to manual gRPC flow control and sets its window.
  */
final case class LedgerClientChannelConfiguration(
    sslContext: Option[SslContext],
    maxInboundMetadataSize: Int = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE,
    maxInboundMessageSize: Int = LedgerClientChannelConfiguration.DefaultMaxInboundMessageSize,
    flowControlWindow: PositiveInt = LedgerClientChannelConfiguration.DefaultFlowControlWindow,
) {

  def builderFor(host: String, port: Int): NettyChannelBuilder =
    builderFor(host, port, LedgerClientChannelConfiguration.DefaultInitialFlowControlWindow)

  /** @param initialFlowControlWindow
    *   Switches to automatic gRPC flow control and sets its initial window; if `None`, then it is
    *   not configured and `flowControlWindow` in `LedgerClientChannelConfiguration` takes effect.
    *   If present, it is set after the `flowControlWindow`, so it overrides it.
    */
  def builderFor(
      host: String,
      port: Int,
      initialFlowControlWindow: Option[PositiveInt],
  ): NettyChannelBuilder = {
    val builder = NettyChannelBuilder
      .forAddress(host, port)
    sslContext
      .fold(builder.usePlaintext())(builder.sslContext(_).negotiationType(NegotiationType.TLS))
      .maxInboundMetadataSize(maxInboundMetadataSize)
      .maxInboundMessageSize(maxInboundMessageSize)
      .flowControlWindow(flowControlWindow.unwrap)

    // Leveraging mutable builder for conciseness
    initialFlowControlWindow.map(_.unwrap).map(builder.initialFlowControlWindow).discard

    builder
  }
}

object LedgerClientChannelConfiguration {

  val DefaultMaxInboundMessageSize: Int = 10 * 1024 * 1024
  // Manual flow control window size, used if auto flow control is disabled
  val DefaultFlowControlWindow: PositiveInt = PositiveInt.tryCreate(1024 * 1024)
  // Explicit auto flow control with 1MB initial window size
  val DefaultInitialFlowControlWindow: Option[PositiveInt] =
    Some(PositiveInt.tryCreate(1024 * 1024))
  val InsecureDefaults: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(sslContext = None)

}
