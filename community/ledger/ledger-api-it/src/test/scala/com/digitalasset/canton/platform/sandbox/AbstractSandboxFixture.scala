// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.resources.ResourceOwner
import com.daml.ports.Port
import com.daml.testing.utils.{TestModels, TestResourceUtils}
import com.digitalasset.canton.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.canton.ledger.api.auth.{AuthService, JwtVerifierLoader}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.client.services.testing.time.StaticTime
import com.digitalasset.canton.ledger.runner.common.Config
import com.digitalasset.canton.ledger.sandbox.BridgeConfig
import com.digitalasset.canton.ledger.sandbox.SandboxOnXForTest.{
  ApiServerConfig,
  Default,
  DevEngineConfig,
  singleParticipant,
}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.sandbox.services.DbInfo
import com.digitalasset.canton.platform.services.time.TimeProviderType
import io.grpc.Channel
import org.scalatest.Suite
import scalaz.syntax.tag.*

import java.io.File
import java.net.InetAddress
import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Try

trait AbstractSandboxFixture extends AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def darFile =
    TestResourceUtils.resourceFile(TestModels.com_daml_ledger_test_ModelTestDar_1_15_path)

  protected def ledgerId(token: Option[String] = None): domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .withCallCredentials(token.map(new LedgerCallCredentials(_)).orNull)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId: @nowarn(
        "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
      )
    )

  protected def getTimeProviderForClient(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
  ): TimeProvider = {
    Try(TimeServiceGrpc.stub(channel))
      .map(StaticTime.updatedVia(_, ledgerId().unwrap)(mat, esf))
      .fold[TimeProvider](_ => TimeProvider.UTC, Await.result(_, 30.seconds))
  }

  def bridgeConfig: BridgeConfig = BridgeConfig()

  protected def config: Config = Default.copy(
    ledgerId = "sandbox-server",
    engine = DevEngineConfig,
    participants = singleParticipant(
      ApiServerConfig.copy(
        seeding = Seeding.Weak,
        timeProviderType = TimeProviderType.Static,
      )
    ),
  )

  protected def packageFiles: List[File] = List(darFile)

  protected def authService: Option[AuthService] = None

  protected def idpJwtVerifierLoader: Option[JwtVerifierLoader] = None

  protected def scenario: Option[String] = None

  protected def database: Option[ResourceOwner[DbInfo]] = None

  protected def serverHost: String = InetAddress.getLoopbackAddress.getHostName

  protected def serverPort: Port

  protected def channel: Channel
}
