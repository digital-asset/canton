// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import com.daml.grpc.adapter.utils.implementations.HelloServicePekkoImplementation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, TestExecutionSequencerFactory}
import com.daml.platform.hello.HelloServiceGrpc
import com.daml.platform.hello.HelloServiceGrpc.HelloServiceStub
import com.daml.testing.utils.{
  GrpcServerFixture,
  PekkoStreamGrpcServerResource,
  Resource,
  ServerWithChannelProvider,
  SuiteResourceManagementAroundAll,
}

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

trait PekkoServiceFixture
    extends GrpcServerFixture[HelloServiceStub]
    with SuiteResourceManagementAroundAll {

  protected lazy val channel = suiteResource.value.channel()
  protected lazy val clientStub = HelloServiceGrpc.stub(channel)

  // Stop the server (see `ServerWithChannelProvider.shutdownServer`) before closing the client
  // channel, so in-flight streams can end before their connection goes away.
  override protected def afterAll(): Unit = {
    val clientChannel = channel
    try super.afterAll()
    finally {
      clientChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
      ()
    }
  }
  protected def socketAddress: Option[SocketAddress]

  override protected def suiteResource: Resource[ServerWithChannelProvider] = resources.map(_._2)

  lazy val resources = PekkoServiceFixture.getResource(socketAddress)

  protected def service: HelloServicePekkoImplementation =
    resources.getRunningServices.head.asInstanceOf[HelloServicePekkoImplementation]

}

object PekkoServiceFixture {

  implicit private val esf: ExecutionSequencerFactory = TestExecutionSequencerFactory.instance

  def getResource(address: Option[SocketAddress]): PekkoStreamGrpcServerResource =
    PekkoStreamGrpcServerResource(
      implicit m => List(new HelloServicePekkoImplementation()),
      "serverPekkoServiceFixture",
      address,
    )
}
