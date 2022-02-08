// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.tracing.Traced
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class GrpcSequencerInitializationServiceTest extends AsyncWordSpec with BaseTest {
  val domainId = DefaultTestIdentities.domainId
  val identityFactory = TestingTopology(domains = Set(domainId)).build(loggerFactory)
  val sequencerKey = SymbolicCrypto.signingPublicKey("seq-key")
  val initRequest =
    v0.InitRequest(
      domainId.toProtoPrimitive,
      None,
      Some(TestDomainParameters.defaultStatic.toProtoV0),
      None,
    )

  def createSut(initialize: Traced[InitRequest] => EitherT[Future, String, InitResponse]) =
    new GrpcSequencerInitializationService(initialize, loggerFactory)

  "GrpcSequencerInitializationService" should {
    "call given initialize function " in {
      val sut =
        createSut(_ => EitherT.rightT[Future, String](InitResponse("test", sequencerKey, false)))
      for {
        response <- sut.init(initRequest)
      } yield {
        response shouldBe v0.InitResponse("test", Some(sequencerKey.toProtoV0), false)
      }
    }
  }
}
