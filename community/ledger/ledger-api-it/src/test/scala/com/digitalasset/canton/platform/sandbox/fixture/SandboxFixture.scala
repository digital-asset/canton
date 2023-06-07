// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.fixture

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ports.Port
import com.digitalasset.canton.ledger.api.grpc.GrpcClientResource
import com.digitalasset.canton.ledger.sandbox.SandboxOnXForTest.{ConfigAdaptor, dataSource}
import com.digitalasset.canton.ledger.sandbox.{SandboxOnXForTest, SandboxOnXRunner}
import com.digitalasset.canton.platform.sandbox.UploadPackageHelper.*
import com.digitalasset.canton.platform.sandbox.{
  AbstractSandboxFixture,
  SandboxRequiringAuthorizationFuns,
}
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration.*

trait SandboxFixture
    extends AbstractSandboxFixture
    with SuiteResource[(Port, Channel)]
    with SandboxRequiringAuthorizationFuns {
  self: Suite =>

  override protected def serverPort: Port = suiteResource.value._1

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(Port, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(
            _.map(info => Some(info.jdbcUrl))
          )

        cfg = config.withDataSource(
          dataSource(jdbcUrl.getOrElse(SandboxOnXForTest.defaultH2SandboxJdbcUrl()))
        )
        port <- SandboxOnXRunner.owner(
          ConfigAdaptor(authService, idpJwtVerifierLoader),
          cfg,
          bridgeConfig,
          registerGlobalOpenTelemetry = false,
        )
        channel <- GrpcClientResource.owner(port)
        client = adminLedgerClient(port, cfg, jwtSecret)(
          system.dispatcher,
          executionSequencerFactory,
        )
        _ <- ResourceOwner.forFuture(() => uploadDarFiles(client, packageFiles)(system.dispatcher))
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
