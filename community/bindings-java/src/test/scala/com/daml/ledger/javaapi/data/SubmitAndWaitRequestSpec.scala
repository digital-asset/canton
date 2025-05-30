// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.CommandServiceOuterClass.SubmitAndWaitRequest as SubmitAndWaitRequestProto
import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SubmitAndWaitRequestSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitAndWaitRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    commandsGen.map(SubmitAndWaitRequestProto.newBuilder().setCommands(_).build)
  ) { request =>
    val commands = SubmitAndWaitRequest.fromProto(request)
    SubmitAndWaitRequest.fromProto(SubmitAndWaitRequest.toProto(commands)) shouldEqual commands
  }
}
