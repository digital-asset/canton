// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import com.daml.grpc.sampleservice.implementations.HelloServiceReferenceImplementation
import com.daml.platform.hello.HelloServiceGrpc
import com.daml.platform.hello.HelloServiceGrpc.HelloServiceStub
import com.daml.testing.utils.{GrpcServiceFixture, SuiteResourceManagementAroundAll}

import java.util.concurrent.TimeUnit

trait ReferenceServiceFixture
    extends GrpcServiceFixture[HelloServiceStub]
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

  override protected def services =
    List(new HelloServiceReferenceImplementation())

}
