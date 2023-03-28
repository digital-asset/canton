// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox

import com.daml.grpc.adapter.{ExecutionSequencerFactory, SingleThreadExecutionSequencerPool}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.VersionRange
import com.daml.lf.language.LanguageVersion
import com.daml.ports.Port
import com.daml.testing.utils.{TestModels, TestResourceUtils}
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.digitalasset.canton.ledger.runner.common.Config
import com.digitalasset.canton.ledger.sandbox.SandboxOnXForTest.*
import com.digitalasset.canton.ledger.sandbox.{BridgeConfig, SandboxOnXForTest, SandboxOnXRunner}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.services.GrpcClientResource
import com.digitalasset.canton.platform.sandbox.fixture.SandboxFixture
import com.google.protobuf
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.util.{Failure, Success}

class EngineModeIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with TestResourceContext
    with SandboxFixture {
  private[this] implicit val esf: ExecutionSequencerFactory =
    new SingleThreadExecutionSequencerPool("testSequencerPool")

  private[this] val applicationId = ApplicationId("EngineModeIT")

  private[this] val party = UUID.randomUUID.toString

  private[this] def ledgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = None,
    )

  private[this] def buildRequest(pkgId: String, ledgerId: LedgerId) = {
    import scalaz.syntax.tag.*
    val tmplId = Some(Identifier(pkgId, "UnitMod", "Box"))
    val cmd = Command().withCreate(
      CreateCommand(
        templateId = tmplId,
        createArguments = Some(
          Record(
            tmplId,
            Seq(
              RecordField(value = Some(Value().withUnit(protobuf.empty.Empty()))),
              RecordField(value = Some(Value().withParty(party))),
            ),
          )
        ),
      )
    )
    SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = applicationId.unwrap,
          ledgerId = ledgerId.unwrap,
          commandId = UUID.randomUUID.toString,
          commands = Seq(cmd),
        )
      )
    )
  }

  private[this] def run(darPath: Path, serverPort: Port) =
    (
      for {
        channel <- GrpcClientResource.owner(serverPort).acquire().asFuture
        client <- LedgerClient.apply(channel, ledgerClientConfiguration)
        darContent = protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
        pkgsBefore <- client.packageManagementClient.listKnownPackages()
        _ = pkgsBefore shouldBe empty
        _ <- client.packageManagementClient.uploadDarFile(darContent)
        pkgsAfter <- client.packageManagementClient.listKnownPackages()
        _ = pkgsAfter.size shouldBe 1
        _ = client.partyManagementClient.allocateParty(Some(party), None)
        // Uploading the package is not enough.
        // We have to submit a request that forces the engine to load the package.
        request = buildRequest(pkgsAfter.head.packageId, client.ledgerId)
        resp <- client.commandServiceClient.submitAndWaitForTransactionId(request)
      } yield Success(resp.transactionId)
    ).recover { case x => Failure(x) }

  "SandboxServer" should {
    def buildServer(versions: VersionRange[LanguageVersion]) = {

      val bridgeConfig: BridgeConfig = BridgeConfig()

      val sandboxConfig: Config = Default.copy(
        ledgerId = "ledger-server",
        engine = Default.engine.copy(
          allowedLanguageVersions = versions
        ),
        participants = singleParticipant(
          ApiServerConfig.copy(
            seeding = Seeding.Weak
          )
        ),
        dataSource = dataSource(SandboxOnXForTest.defaultH2SandboxJdbcUrl()),
      )
      SandboxOnXRunner.owner(
        ConfigAdaptor(authService),
        sandboxConfig,
        bridgeConfig,
        registerGlobalOpenTelemetry = false,
      )
    }

    def load(langVersion: LanguageVersion, range: VersionRange[LanguageVersion]) =
      buildServer(range).use(
        run(
          TestResourceUtils
            .resourceFile(TestModels.daml_lf_encoder_test_dar(langVersion.pretty))
            .toPath,
          _,
        )
      )

    def accept(langVersion: LanguageVersion, range: VersionRange[LanguageVersion], mode: String) =
      s"accept LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, range).map {
          inside(_) { case Success(_) =>
            succeed
          }
        }

    def reject(langVersion: LanguageVersion, range: VersionRange[LanguageVersion], mode: String) =
      s"reject LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, range).map {
          inside(_) { case Failure(exception) =>
            exception.getMessage should include("Disallowed language version")
          }
        }

    inside(
      List(
        LanguageVersion.StableVersions.max,
        LanguageVersion.EarlyAccessVersions.max,
        LanguageVersion.DevVersions.max,
      )
    ) { case List(maxStableVersion, previewVersion, devVersion) =>
      accept(maxStableVersion, LanguageVersion.StableVersions, "stable")
      accept(maxStableVersion, LanguageVersion.EarlyAccessVersions, "early access")
      accept(maxStableVersion, LanguageVersion.DevVersions, "dev")

      if (LanguageVersion.EarlyAccessVersions != LanguageVersion.StableVersions) {
        // a preview version is currently available
        reject(previewVersion, LanguageVersion.StableVersions, "stable")
        accept(previewVersion, LanguageVersion.EarlyAccessVersions, "early access")
        accept(previewVersion, LanguageVersion.DevVersions, "dev")
      }

      reject(devVersion, LanguageVersion.StableVersions, "stable")
      reject(devVersion, LanguageVersion.EarlyAccessVersions, "early access")
      accept(devVersion, LanguageVersion.DevVersions, "dev")
    }
  }

}
