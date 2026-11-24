// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.{
  CannotParseFilesError,
  CannotReadFilesError,
  CantonConfigError,
  GenericConfigError,
  NoConfigFiles,
  SubstitutionError,
}
import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.DeprecatedApiGate
import com.digitalasset.canton.ledger.api.validation.LedgerApiSizeLimitsConfig
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{ErrorLoggingContext, LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.config.ExtensionServiceAuthConfig
import com.digitalasset.canton.platform.config.{
  TopUpConfig,
  TrafficAccountingConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.ThroughputCapConfig
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.{
  ApiDeprecation,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import com.digitalasset.nonempty.NonEmpty
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.ConfigSource

import java.io.File as JFile
import java.nio.file.Path

class CantonConfigTest extends AnyWordSpec with BaseTest {

  import scala.jdk.CollectionConverters.*

  private lazy val baseDir: File = "community" / "app" / "src" / "test" / "resources"
  private lazy val examplesDir: File = "community" / "app" / "src" / "pack" / "examples"
  private lazy val confDir: File = "community" / "app" / "src" / "pack" / "config"
  private lazy val node: File = confDir / "participant.conf"
  private lazy val env: File =
    "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env"
  private lazy val perf: File = "scripts" / "canton-testing"

  lazy val files: Seq[JFile] = Seq(env, node).map(_.toJava)

  private lazy val deprecatedConfigs: File =
    "community" / "app" / "src" / "test" / "resources" / "deprecated-configs"

  private lazy val remoteSequencerHAConfig: File =
    baseDir / "external-ha-sequencers-remote-admin-parsing.conf"

  private lazy val distributedConf: File = baseDir / "distributed-single-synchronizer-topology.conf"

  private lazy val simpleConf: File = examplesDir / "01-simple-topology" / "simple-topology.conf"
  private lazy val simpleConfPath: String = simpleConf.pathAsString

  private lazy val observabilityDir: File = examplesDir / "13-observability" / "canton"
  private lazy val observabilityNetworkConf: File = observabilityDir / "network.conf"
  private lazy val observabilitySequencer4Conf: File = observabilityDir / "sequencer4.conf"

  private lazy val duplicateStorageWithoutReplicationConfig: File =
    baseDir / "duplicate-storage-without-replication.conf"

  private lazy val duplicateStorageWithReplicationConfig: File =
    baseDir / "duplicate-storage-with-replication.conf"

  private lazy val unsupportedMinProtocolVersionConfig: File =
    baseDir / "unsupported-minimum-protocol-version.conf"

  private def loadFile(resourcePath: String): Either[CantonConfigError, CantonConfig] =
    loadFiles(Seq(resourcePath))

  private def loadFiles(
      resourcePaths: Seq[String]
  ): Either[CantonConfigError, CantonConfig] = {
    val files = resourcePaths.map(r => (baseDir.toString / r).toJava)
    CantonConfig.parseAndLoad(files, Some(DefaultPorts.create()))
  }

  "the invalid node names configuration" should {
    "return an error" in {
      loggerFactory.assertLogs(
        {
          val result = loadFile("invalid-configs/invalid-node-names.conf")
          inside(result.left.value) { case GenericConfigError.Error(cause) =>
            cause should include(
              "Node name is too long. Max length: 30. Length: 43. Name: \"myparticipant01234567890123456...\""
            )
            cause should include(
              "Node name contains invalid characters (allowed: [a-zA-Z0-9_-]): \"my`participant\""
            )
            cause should include(
              "Node name contains invalid characters (allowed: [a-zA-Z0-9_-]): \"my participant\""
            )
          }
        },
        entry => {
          entry.shouldBeCantonErrorCode(GenericConfigError.code)
          val cause = entry.errorMessage
          cause should include(
            "Node name is too long. Max length: 30. Length: 43. Name: \"myparticipant01234567890123456...\""
          )
          // The other causes get truncated away, unfortunately.
          // See https://github.com/digital-asset/daml/issues/12785
        },
      )
    }
  }

  // test that fails because we misspelled 'port' as 'bort'
  "the bort configuration" should {
    "return an error mentioning the bort issue" in {
      val result = loggerFactory.assertLogs(
        loadFiles(Seq(simpleConfPath, "invalid-configs/bort.conf")),
        _.errorMessage should (include("bort.conf") and include("Unknown key")),
      )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  // test that fails because of missing '{' in .conf-file
  "the missing-bracket configuration" should {
    "return a CannotParseFilesError during loading when combined with simple config" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/missing-bracket.conf")),
          _.mdc("err-context") should (include("missing-bracket.conf") and include(
            "expecting a close parentheses ')' here, not: end of file"
          )),
        )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }

    "return a CannotParseFilesError during loading when not combined with simple config" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/missing-bracket.conf")),
          _.mdc("err-context") should (include("missing-bracket.conf") and include(
            "expecting a close parentheses ')' here, not: end of file"
          )),
        )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }
  }

  "sequencer throughput cap configuration" should {
    def loadCaps(config: String) = {
      val readers = new CantonConfig.ConfigReaders()(
        ErrorLoggingContext.fromTracedLogger(logger)
      )
      import readers.throughputCapConfigReader
      ConfigSource.string(config).load[ThroughputCapConfig]
    }

    def loadLegacyCaps(config: String): ThroughputCapConfig =
      loggerFactory.assertLogs(
        loadCaps(config).value,
        _.warningMessage should include("throughput-cap configuration is deprecated"),
      )

    "project legacy limits and inherit the parent observation period" in {
      val config = loadLegacyCaps("""
        enabled = true
        observation-period-seconds = 17
        clock-tick-interval = 250ms
        strict = false
        delayed-activation = true
        thresholds = [0.8, 0.6]
        update-every-ms = 250
        messages {
          confirmation-request {
            global-tps-cap = 30
            global-kbps-cap = 3000
            per-client-tps-cap = 3
            per-client-kbps-cap = 300
          }
          topology.per-client-tps-cap = 0.5
          confirmation-response.global-tps-cap = 90
        }
      """)
      config.enabled shouldBe true
      config.strict shouldBe false
      config.clockTickInterval shouldBe NonNegativeFiniteDuration.ofMillis(250)
      config.updateEveryMs.value shouldBe 250
      config.thresholds.map(_.value) shouldBe Seq(0.8, 0.6)
      val request = config.messages("confirmation-request")
      request.requestType shouldBe SubmissionRequestType.ConfirmationRequest
      request.globalTpsCap.value shouldBe 30
      request.globalKbpsCap.value shouldBe 3000
      request.perClientTpsCap.value shouldBe 3
      request.perClientKbpsCap.value shouldBe 300
      config.messages("topology").requestType shouldBe SubmissionRequestType.TopologyTransaction
      config.messages("topology").perClientTpsCap.value shouldBe 0.5
      config
        .messages("confirmation-response")
        .requestType shouldBe SubmissionRequestType.ConfirmationResponse
      config.messages("confirmation-response").globalTpsCap.value shouldBe 90
      Seq("confirmation-request", "topology", "confirmation-response").foreach { name =>
        config.messages(name).observationPeriodSeconds.value shouldBe 17
      }
      config.messages("commitments") shouldBe ThroughputCapConfig().messages("commitments")
      config.messages("time-proofs") shouldBe ThroughputCapConfig().messages("time-proofs")
    }

    "preserve omitted legacy limits and the default parent observation period" in {
      val config = loadLegacyCaps("messages.confirmation-request.per-client-tps-cap = 7")
      config.messages shouldBe ThroughputCapConfig().messages.updated(
        "confirmation-request",
        ThroughputCapConfig()
          .messages("confirmation-request")
          .copy(
            perClientTpsCap = RequireTypes.PositiveDouble.tryCreate(7)
          ),
      )
    }

    "project the parent observation period even when no messages are specified" in {
      val config = loadLegacyCaps("observation-period-seconds = 23")
      Seq("confirmation-request", "topology", "confirmation-response").foreach { name =>
        config.messages(name).observationPeriodSeconds.value shouldBe 23
      }
    }

    "accept the legacy delayed activation field without restoring the grace period" in {
      loadLegacyCaps("delayed-activation = false") shouldBe ThroughputCapConfig()
    }

    "keep named settings and their independent periods unchanged without warning" in {
      val config = loadCaps("""
        messages {
          short {
            request-type = "confirmation request"
            observation-period-seconds = 2
          }
          long {
            request-type = "confirmation request"
            observation-period-seconds = 19
          }
        }
      """).value
      config.messages.keySet shouldBe Set("short", "long")
      config.messages("short").observationPeriodSeconds.value shouldBe 2
      config.messages("long").observationPeriodSeconds.value shouldBe 19
      loadCaps("").value shouldBe ThroughputCapConfig()
      loadCaps("messages = {}").value.messages shouldBe empty
    }

    "reject invalid legacy values and unknown or misspelled settings" in {
      Seq(
        "observation-period-seconds = 0",
        "observation-period-seconds = -1",
        "messages.confirmation-request.per-client-tps-capp = 1",
        "messages.custom.per-client-tps-cap = 1",
        """messages.confirmation-request.request-type = "invalid"""",
      ).foreach(config => loadCaps(config).isLeft shouldBe true)
    }
  }

  "the negative-port configuration" should {

    "return a sensible error message during loading" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/negative-port.conf")),
          _.errorMessage should (include("negative-port.conf") and include("Unable to create Port")),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  "the traffic accounting top-up configuration" should {

    def damlTopUp(cfg: TrafficAccountingConfig): Option[TopUpConfig.DamlCredit] =
      Some(cfg.trafficEnforcementServer).flatMap {
        case internal: TrafficEnforcementServerConfig.Internal =>
          Some(internal.topUp).collect { case damlCredit: TopUpConfig.DamlCredit => damlCredit }
      }

    "default to disabled" in {
      val config = loadFiles(Seq(simpleConfPath)).value
      config.participants.values.map(cfg =>
        damlTopUp(cfg.trafficAccounting)
      ) should contain only None
    }

    "load the Daml credit top-up" in {
      val config =
        loadFiles(Seq(simpleConfPath, "traffic-accounting-daml-top-up.conf")).value
      val participant = config.participants.get(InstanceName.tryCreate("participant1")).value
      damlTopUp(participant.trafficAccounting) shouldBe Some(
        TopUpConfig.DamlCredit(
          paymasters = NonEmpty(
            Set,
            PartyId.tryFromProtoPrimitive("paymaster::1220aa"),
            PartyId.tryFromProtoPrimitive("paymaster::1220bb"),
          ),
          initialOffsetBeginExclusive = Some(42L),
        )
      )
    }

    "reject a Daml credit top-up without paymasters" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(
            Seq(simpleConfPath, "invalid-configs/traffic-accounting-empty-paymasters.conf")
          ),
          _.errorMessage should (include("traffic-accounting-empty-paymasters.conf") and include(
            "paymasters"
          )),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  // test that fails because of using env variable substitution with a non-existent env variable
  "the undefined-env-var configuration" should {

    "return an error during loading" in {
      // defined like this because instantiating the error will automatically lead to another error message being logged
      val code = loggerFactory.assertLogs(
        SubstitutionError.Error(Seq()).code.id,
        _.message should include(""),
      )
      val result = loggerFactory.assertLogs(
        loadFile("invalid-configs/undefined-env-var.conf"),
        logEntry => {
          logEntry.mdc("err-context") should (include("UNDEFINED_ENV_VARIABLE") and include(
            "undefined-env-var.conf"
          ))
          logEntry.errorMessage should include(code)
        },
      )
      result.left.value shouldBe a[SubstitutionError.Error]
    }
  }

  // confs with missing files for includes
  // no error despite missing include
  "the include-missing-file configuration" should {
    lazy val config =
      loadFiles(Seq(simpleConfPath, "invalid-configs/include-missing-file.conf"))
        .valueOrFail("failed to load include-missing-file.conf")

    "contain a couple of participants2" in {
      config.participants should have size 2
    }
  }

  // tests that fails because of a `include required` of a missing file
  "the require-missing-file configuration" should {
    "throw a meaningful error message during loading" in {
      // sadly, we don't have enough information at the time the error is thrown to also include
      // `require-missing-file.conf` in the error message
      val result = loggerFactory.assertLogs(
        loadFiles(Seq("invalid-configs/require-missing-file.conf", simpleConfPath)),
        _.mdc("err-context") should (include("this-file-does-not-exist.conf") and include(
          "resource not found"
        )),
      )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }
  }

  "configuration file with unknown keys" should {
    "return an error" in {
      val result =
        loggerFactory.assertLogs(
          loadFile("invalid-configs/unknown-key-in-nested-config.conf"),
          _.errorMessage should include("canton.monitoring.this-is-not-a-key"),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }

    "not return an error if unknown keys are allowed" in {
      val filename = "invalid-configs/unknown-key-allowed-in-nested-config.conf"
      val config = loadFile(filename).valueOrFail(s"failed to load $filename")

      config.participants.keySet shouldBe Set(InstanceName.tryCreate("participant1"))
    }
  }

  // Technically it's the same scenario as above,
  // but it wasn't covered before https://github.com/DACH-NY/canton/issues/33636
  "configuration file with no-longer-existing keys" should {
    "return an error" in {
      val result =
        loggerFactory.assertLogs(
          loadFile("invalid-configs/no-longer-existing-key.conf"),
          _.errorMessage should include("ledger-api-jdbc-url"),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }

    "not return an error if unknown keys are allowed" in {
      val filename = "invalid-configs/no-longer-existing-allowed-key.conf"
      val config = loadFile(filename).valueOrFail(s"failed to load $filename")

      config.participants.keySet shouldBe Set(InstanceName.tryCreate("participant1"))
    }
  }

  "the ledger api size limits configuration" should {

    def withSizeLimits(sizeLimits: String)(
        check: Either[CantonConfigError, LedgerApiSizeLimitsConfig] => Assertion
    ): Unit =
      File.usingTemporaryFile("size-limits", ".conf") { overrideFile =>
        overrideFile.writeText(
          s"canton.participants.participant1.ledger-api.size-limits { $sizeLimits }"
        )
        check(
          CantonConfig
            .parseAndLoad(Seq(simpleConf.toJava, overrideFile.toJava), defaultPorts = None)
            .map(_.participantsByString("participant1").ledgerApi.sizeLimits)
        )
      }

    "keep the default limits for the limits that are not set" in withSizeLimits(
      "public-lapi-size-config.completion-stream-request.parties-size-limit = 10"
    ) { result =>
      val sizeLimits = result.value.publicLapiSizeConfig
      val defaultSizeLimits = LedgerApiSizeLimitsConfig().publicLapiSizeConfig

      sizeLimits.completionStreamRequest.partiesSizeLimit shouldBe PositiveInt.tryCreate(10)
      sizeLimits.copy(completionStreamRequest = defaultSizeLimits.completionStreamRequest) shouldBe
        defaultSizeLimits
    }

    "apply a shared limit to the linked limits that are not set" in withSizeLimits(
      """shared-size-config.completions-request-parties-size-limit = 5
        |public-lapi-size-config.completion-stream-request.parties-size-limit = 10""".stripMargin
    ) { result =>
      val sizeLimits = result.value
      inside(sizeLimits) { case LedgerApiSizeLimitsConfig(_, _, _, sharedSizeConfig, _, _) =>
        sharedSizeConfig.completionsRequestPartiesSizeLimit shouldBe PositiveInt.tryCreate(5)
      }
      sizeLimits.publicLapiSizeConfig.getCompletionsRequest.partiesSizeLimit shouldBe
        PositiveInt.tryCreate(5)
      // the message-specific limit that is set takes over the shared limit
      sizeLimits.publicLapiSizeConfig.completionStreamRequest.partiesSizeLimit shouldBe
        PositiveInt.tryCreate(10)
    }

    "apply a shared limit to the linked admin, interactive and custom limits" in withSizeLimits(
      """shared-size-config.signatures-size-limit = 3
        |shared-size-config.submission-act-as-size-limit = 4
        |shared-size-config.contract-bytes-size-limit = 5
        |admin-lapi-size-config.authorize-party-update-request.signatures-size-limit = 6""".stripMargin
    ) { result =>
      val sizeLimits = result.value
      val admin = sizeLimits.adminLapiSizeConfig
      val interactive = sizeLimits.interactiveLapiSizeConfig

      admin.allocateExternalPartyRequestSignedTransaction.signaturesSizeLimit shouldBe
        PositiveInt.tryCreate(3)
      admin.allocateExternalPartyRequest.multiHashSignaturesSizeLimit shouldBe
        PositiveInt.tryCreate(3)
      interactive.singlePartySignatures.signaturesSizeLimit shouldBe PositiveInt.tryCreate(3)
      // the message-specific limit that is set takes over the shared limit
      admin.authorizePartyUpdateRequest.signaturesSizeLimit shouldBe PositiveInt.tryCreate(6)

      sizeLimits.publicLapiSizeConfig.commands.actAsSizeLimit shouldBe PositiveInt.tryCreate(4)
      interactive.prepareSubmissionRequest.actAsSizeLimit shouldBe PositiveInt.tryCreate(4)
      interactive.metadataSubmitterInfo.actAsSizeLimit shouldBe PositiveInt.tryCreate(4)

      sizeLimits.publicLapiSizeConfig.disclosedContract.createdEventBlobSizeLimit shouldBe
        PositiveInt.tryCreate(5)
      sizeLimits.metadataInputContractBytesSizeLimit shouldBe PositiveInt.tryCreate(5)
    }

    "read the event format total filters size limit" in withSizeLimits(
      "event-format-total-filters-size-limit = 7"
    ) { result =>
      result.value.eventFormatTotalFiltersSizeLimit shouldBe PositiveInt.tryCreate(7)
    }

    "reject an invalid shared limit" in loggerFactory.assertLogs(
      withSizeLimits("shared-size-config.completions-request-parties-size-limit = -1") {
        _.left.value shouldBe a[GenericConfigError.Error]
      },
      _.errorMessage should include("shared-size-config.completions-request-parties-size-limit"),
    )

    "reject an unknown key for PublicLapiSizeConfig" in loggerFactory.assertLogs(
      withSizeLimits("public-lapi-size-config.completion-stream-request.this-is-not-a-key = 10") {
        _.left.value shouldBe a[GenericConfigError.Error]
      },
      _.errorMessage should include("this-is-not-a-key"),
    )

    "reject an unknown shared key" in loggerFactory.assertLogs(
      withSizeLimits("shared-size-config.this-is-not-a-key = 10") {
        _.left.value shouldBe a[GenericConfigError.Error]
      },
      _.errorMessage should include("this-is-not-a-key"),
    )
  }

  "load with multiple config files" should {
    lazy val config1: Config = ConfigFactory.parseMap(
      Map(
        "item1" -> "config1",
        "item2" -> "config1",
      ).asJava
    )
    lazy val config2: Config = ConfigFactory.parseMap(
      Map(
        "item2" -> "config2",
        "item3" -> "config2",
      ).asJava
    )
    lazy val combinedConfig = CantonConfig.mergeConfigs(config1, Seq(config2))
    "prefer the right hand config where multiple keys are defined" in {
      combinedConfig.getString("item1") shouldBe "config1"
      // this is defined in both, but as config2 was provided last it should provide the value
      combinedConfig.getString("item2") shouldBe "config2"
      // this is missing from config1
      combinedConfig.getString("item3") shouldBe "config2"
    }

    "load with no config files" should {
      "return None" in {
        val result =
          loggerFactory.assertLogs(
            loadFiles(Seq()),
            _.errorMessage should include("No config files"),
          )

        result.left.value shouldBe a[NoConfigFiles.Error]

      }
    }

    "load with files that cannot be read" should {
      "will log errors for all files that can't be read" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq("file-1", "file-2")),
          _.mdc("err-context") should (include("file-1") and include("file-2")),
        )
        result.left.value shouldBe a[CannotReadFilesError.Error]
      }
    }

    "config validation on duplicate storage" should {

      "not log the password when url or jdbcUrl is set" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/storage-url-with-password.conf")),
          _.errorMessage should (include("Failed to validate the configuration")
            and include("participant1") and include("participant2")
            and not include "password=" and not include "supersafe"),
        )
        result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
      }
    }
  }

  // TODO (i#33090): remove together with the scope-only token format
  "the scope-only JWT feature flag" should {
    def loadWith(extraConfig: String): Either[CantonConfigError, CantonConfig] =
      CantonConfig.loadAndValidate(
        ConfigFactory
          .parseString(extraConfig)
          .withFallback(ConfigFactory.parseFile(simpleConf.toJava))
          .withFallback(ConfigFactory.load()),
        Some(DefaultPorts.create()),
      )

    val scopeOnlyLedgerApi =
      """canton.participants.participant1.ledger-api.auth-services = [{
        |  type = unsafe-jwt-hmac-256
        |  secret = "not-safe-for-production"
        |  target-scope = "my-scope"
        |}]
        |""".stripMargin
    val scopeOnlyAdminApi =
      """canton.sequencers.sequencer1.admin-api.auth-services = [{
        |  type = unsafe-jwt-hmac-256
        |  secret = "not-safe-for-production"
        |  target-scope = "my-scope"
        |}]
        |""".stripMargin
    val flagOn = s"${DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey} = true\n"

    "be disabled by default" in {
      val config = loadWith("").valueOrFail("config should load")
      config.features.deprecated.enableScopeOnlyJwt shouldBe false
      config.participantNodeParametersByString("participant1").enableScopeOnlyJwt shouldBe false
      config.sequencerNodeParametersByString("sequencer1").enableScopeOnlyJwt shouldBe false
    }

    "reach the node parameters of every node when enabled" in {
      val config = loadWith(flagOn).valueOrFail("config should load")
      config.features.deprecated.enableScopeOnlyJwt shouldBe true
      config.participantNodeParametersByString("participant1").enableScopeOnlyJwt shouldBe true
      config.sequencerNodeParametersByString("sequencer1").enableScopeOnlyJwt shouldBe true
      config.mediatorNodeParametersByString("mediator1").enableScopeOnlyJwt shouldBe true
    }

    "reject a scope-only auth service on the ledger API when disabled" in {
      val result = loggerFactory.assertLogs(
        loadWith(scopeOnlyLedgerApi),
        _.errorMessage should (include("Failed to validate the configuration")
          and include("canton.participants.participant1.ledger-api.auth-services")
          and include(DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey)),
      )
      result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
    }

    "reject a scope-only auth service on an admin API when disabled" in {
      val result = loggerFactory.assertLogs(
        loadWith(scopeOnlyAdminApi),
        _.errorMessage should (include("Failed to validate the configuration")
          and include("canton.sequencers.sequencer1.admin-api.auth-services")
          and include(DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey)),
      )
      result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
    }

    "accept a scope-only auth service when enabled" in {
      loadWith(flagOn + scopeOnlyLedgerApi + scopeOnlyAdminApi)
        .valueOrFail("config should load")
        .discard
    }

    "accept an auth service with both a target scope and a target audience when disabled" in {
      val withAudience = scopeOnlyLedgerApi.replace(
        "target-scope = \"my-scope\"",
        "target-scope = \"my-scope\"\n  target-audience = \"my-audience\"",
      )
      loadWith(withAudience).valueOrFail("config should load").discard
    }
  }

  // Parse validation and content assertions for example configs that do not start a live Canton
  // environment (e.g. Docker-Compose-only examples). Integration tests that actually boot Canton
  // live in ExampleIntegrationTest subclasses co-located with those tests.
  "example config content" should {

    def loadExampleFiles(files: File*): CantonConfig =
      CantonConfig
        .parseAndLoad(files.map(_.toJava), defaultPorts = None)
        .valueOrFail(s"failed to load example config: ${files.map(_.name).mkString(", ")}")

    "01-simple-topology" should {
      lazy val config =
        loadExampleFiles(examplesDir / "01-simple-topology" / "simple-topology.conf")

      "contain at least 2 participants, 1 sequencer and 1 mediator" in {
        config.participants.size should be >= 2
        config.sequencers.size should be >= 1
        config.mediators.size should be >= 1
      }

      "produce the expected port description" in {
        config.portDescription.split(";") should contain theSameElementsAs List(
          "participant1:admin-api=5012,ledger-api=5011",
          "participant2:admin-api=5022,ledger-api=5021",
          "sequencer1:admin-api=5002,public-api=5001",
          "mediator1:admin-api=5202",
        )
      }

      "use warn-level startup memory check" in {
        config.parameters.startupMemoryCheckConfig shouldBe StartupMemoryCheckConfig(
          ReportingLevel.Warn
        )
      }
    }

    "13-observability" should {
      // memory.conf overrides _shared.storage so no Postgres instance is required at parse time
      lazy val config =
        CantonConfig
          .parseAndLoad(
            Seq(
              confDir / "storage" / "memory.conf",
              observabilityNetworkConf,
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .valueOrFail("failed to load 13-observability/canton/network.conf")

      "contain at least 2 participants, 3 sequencers and 2 mediators" in {
        config.participants.size should be >= 2
        config.sequencers.size should be >= 3
        config.mediators.size should be >= 2
      }

      "expose participant1 on the expected ports" in {
        val p1 = config.participantsByString("participant1")
        p1.ledgerApi.port.unwrap shouldBe 10011
        p1.adminApi.port.unwrap shouldBe 10012
        p1.httpLedgerApi.port.unwrap shouldBe 10013
      }

      "expose participant2 on the expected ports" in {
        val p2 = config.participantsByString("participant2")
        p2.ledgerApi.port.unwrap shouldBe 10021
        p2.adminApi.port.unwrap shouldBe 10022
        p2.httpLedgerApi.port.unwrap shouldBe 10023
      }

      "expose sequencers on the expected admin ports" in {
        config.sequencersByString("sequencer1").adminApi.port.unwrap shouldBe 4402
        config.sequencersByString("sequencer2").adminApi.port.unwrap shouldBe 4412
        config.sequencersByString("sequencer3").adminApi.port.unwrap shouldBe 4422
      }

      "expose mediators on the expected admin ports" in {
        config.mediatorsByString("mediator1").adminApi.port.unwrap shouldBe 4602
        config.mediatorsByString("mediator2").adminApi.port.unwrap shouldBe 4612
      }
    }

    "13-observability sequencer4" should {
      lazy val config =
        CantonConfig
          .parseAndLoad(
            Seq(
              confDir / "storage" / "memory.conf",
              observabilitySequencer4Conf,
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .valueOrFail("failed to load 13-observability/canton/sequencer4.conf")

      "contain exactly 1 sequencer" in {
        config.sequencers.size should be >= 1
      }

      "expose sequencer4 on the expected ports" in {
        val s4 = config.sequencersByString("sequencer4")
        s4.publicApi.port.unwrap shouldBe 4431
        s4.adminApi.port.unwrap shouldBe 4432
      }
    }
  }

  "parsing our config example snippets" should {
    "succeed on all examples" in {
      val inputDir = baseDir / "documentation-snippets"

      inputDir
        .list(_.extension.contains(".conf"))
        .foreach(file =>
          loggerFactory.assertLogsUnorderedOptional(
            loadFiles(Seq(simpleConfPath, "documentation-snippets/" + file.name))
              .valueOrFail(
                "failed to load " + file.name
              ),
            LogEntryOptionality.Optional -> (entry =>
              entry.shouldBeCantonErrorCode(DeprecatedProtocolVersion)
            ),
          )
        )
    }
  }

  "the deprecated API feature flags" should {
    // The key named in the errors and docs must be the key the config reader reads.
    val deprecation = ApiDeprecation.Canton37.Endpoints

    def participantGate(files: Seq[File]): DeprecatedApiGate =
      DeprecatedApiGate(
        CantonConfig
          .parseAndLoad(files.map(_.toJava), Some(DefaultPorts.create()))
          .valueOrFail("config should load")
          .participantNodeParametersByString("participant1")
          .deprecatedApis
      )

    "disable the deprecated endpoints by default" in {
      participantGate(Seq(simpleConf)).isEnabled(deprecation) shouldBe false
    }

    "enable the deprecated endpoints when the flag is true" in {
      val flag = File.newTemporaryFile(suffix = ".conf").deleteOnExit()
      val key = deprecation.featureFlag.replace("<participant>", "participant1")
      flag.overwrite(s"$key = true").discard
      participantGate(Seq(simpleConf, flag)).isEnabled(deprecation) shouldBe true
    }
  }

  "dumping config" should {

    /** Loads the base config plus `configOverride`, then asserts that none of the `canaries` show
      * up either in the startup log rendering or the config dump.
      */
    def assertNoLeakage(canaries: Seq[String], configOverride: String = "") = {
      val configFromCliOnly = CantonConfig
        .parseAndMergeJustCLIConfigs(NonEmpty.from(files).value)
        .valueOrFail("config should be loaded")
      val mergedConfig =
        CantonConfig.mergeConfigs(configFromCliOnly, Seq(ConfigFactory.parseString(configOverride)))
      val cantonConfig = CantonConfig
        .loadAndValidate(
          mergedConfig.withFallback(ConfigFactory.load()),
          Some(DefaultPorts.create()),
        )
        .valueOrFail("config should be valid")

      // Sanity check that the canaries were actually picked up
      val unredacted = mergedConfig.resolve().root().render()
      clue("canaries should be in the unredacted config") {
        canaries.filterNot(unredacted.contains) shouldBe empty
      }

      val renderings = Seq(
        "startup log rendering" -> cantonConfig.dumpStringWithoutDefaults(mergedConfig),
        "config dump" -> cantonConfig.dumpString,
      )
      val leaks = for {
        (name, rendered) <- renderings
        canary <- canaries if rendered.contains(canary)
      } yield s"$name contains $canary"
      if (leaks.nonEmpty) fail(leaks.mkString("\n"))
    }

    "not leak confidential information" in {
      // Secrets from the base config files
      val dbPassword = "supersafe"
      val jwtSecret = "not-safe-for-production"
      assertNoLeakage(Seq(dbPassword, jwtSecret))
      clue("with password override") {
        assertNoLeakage(
          Seq(dbPassword, jwtSecret),
          // Test with specifically overridden password as well (typically provided via -C on the CLI)
          s"""canton.participants.participant.storage.config.properties.password = "$dbPassword"""",
        )
      }
    }

    "not leak fixed admin tokens, OTLP client secrets or tracing headers" in {
      val fixedAdminToken = "fixed-admin-token-leak-canary"
      val clientSecret = "otlp-client-secret-leak-canary"
      val tracingHeader = "Bearer tracing-header-leak-canary"

      assertNoLeakage(
        Seq(fixedAdminToken, clientSecret, tracingHeader),
        s"""
           |canton.participants.participant {
           |  ledger-api.admin-token-config.fixed-admin-token = "$fixedAdminToken"
           |  admin-api.admin-token-config.fixed-admin-token = "$fixedAdminToken"
           |}
           |canton.monitoring.metrics.reporters = [{
           |  type = otlp
           |  auth {
           |    type = oauth-client-credentials
           |    token-url = "https://auth.example.com/token"
           |    client-id = "canton"
           |    client-secret = "$clientSecret"
           |  }
           |}]
           |canton.monitoring.tracing.tracer.exporter {
           |  type = otlp
           |  additional-headers { "Authorization" = "$tracingHeader" }
           |}
           |""".stripMargin,
      )
    }

    "not leak remote node tokens, JDBC URL passwords or DAR download headers" in {
      val remoteToken = "remote-participant-token-leak-canary"
      val jdbcUrlPassword = "jdbc-url-password-leak-canary"
      val darHeader = "Bearer dar-header-leak-canary"

      assertNoLeakage(
        Seq(remoteToken, jdbcUrlPassword, darHeader),
        s"""
           |canton.remote-participants.remote {
           |  admin-api.port = 10012
           |  ledger-api.port = 10011
           |  token = "$remoteToken"
           |}
           |canton.participants.participant {
           |  storage.config.properties.url =
           |    "jdbc:postgresql://localhost:5432/canton?user=canton&password=$jdbcUrlPassword"
           |  alpha-dynamic.dars = [{
           |    location = "https://example.com/my.dar"
           |    request-headers { "Authorization" = "$darHeader" }
           |  }]
           |}
           |""".stripMargin,
      )
    }

    "not leak database secrets under alternative Slick, Hikari or JDBC driver keys" in {
      val jdbcUrlPassword = "top-level-jdbc-url-leak-canary"
      val dataSourceUrlPassword = "data-source-url-leak-canary"
      val sslPassword = "ssl-password-leak-canary"

      assertNoLeakage(
        Seq(jdbcUrlPassword, dataSourceUrlPassword, sslPassword),
        s"""
           |canton.participants.participant.storage.config {
           |  jdbcUrl = "jdbc:postgresql://localhost:5432/canton?password=$jdbcUrlPassword"
           |  properties.URL = "jdbc:postgresql://localhost:5432/canton?password=$dataSourceUrlPassword"
           |  properties.sslpassword = "$sslPassword"
           |}
           |""".stripMargin,
      )
    }

    "not leak database passwords with in-memory storage" in {
      val dbPassword = "memory-storage-password-leak-canary"

      assertNoLeakage(
        Seq(dbPassword),
        s"""
           |canton.participants.participant.storage {
           |  type = memory
           |  config.properties.password = "$dbPassword"
           |}
           |""".stripMargin,
      )
    }

    "not include all of the default values on startup" in {
      val configFromCliOnly = CantonConfig
        .parseAndMergeJustCLIConfigs(NonEmpty.from(files).value)
        .valueOrFail("config should be merged")
      val mergedConfig =
        CantonConfig.mergeConfigs(
          configFromCliOnly,
          Seq(
            ConfigFactory.parseString(
              s"""
           |canton.remote-participants.remote {
           |  admin-api.port = 10012
           |  ledger-api.port = 10011
           |  token = "hunter2"
           |}
           |""".stripMargin
            )
          ),
        )
      val config = CantonConfig.parseAndLoadOrExit(files, Some(DefaultPorts.create()))
      val defaultOnlyKey = "log-config-with-defaults"

      config.dumpString should include(defaultOnlyKey)
      val rendered = config.dumpStringWithoutDefaults(mergedConfig)
      rendered should not include defaultOnlyKey
      rendered should include("remote-participants")
    }
  }

  "config file" should {

    "load / save / load" in {
      val config = CantonConfig.parseAndLoadOrExit(
        files,
        Some(DefaultPorts.create()),
      )

      File.usingTemporaryFile("reload-test", ".canton") { file =>
        CantonConfig.save(config, file.toString)
        val loaded = CantonConfig.parseAndLoadOrExit(
          Seq(file.toJava),
          Some(DefaultPorts.create()),
        )
        config shouldBe loaded
      }
    }

    "load bearer token file auth for participant engine extensions" in {
      File.usingTemporaryFile("extension-token", ".txt") { tokenFile =>
        tokenFile.writeText("test-token")

        File.usingTemporaryFile("extension-service", ".conf") { overrideFile =>
          overrideFile.writeText(
            s"""
               |canton.participants.participant1.parameters.engine.extensions.external-call-test {
               |  address = "127.0.0.1"
               |  port = 12345
               |  version = "v-test"
               |  tls.enabled = false
               |
               |  auth {
               |    type = bearer-token-file
               |    token-file = "${tokenFile.pathAsString}"
               |  }
               |}
               |""".stripMargin
          )

          val config = CantonConfig.parseAndLoadOrExit(
            Seq(simpleConf.toJava, overrideFile.toJava),
            defaultPorts = None,
          )

          val extension = config
            .participantsByString("participant1")
            .parameters
            .engine
            .extensions("external-call-test")
          extension.version shouldBe "v-test"

          inside(extension.auth) { case ExtensionServiceAuthConfig.BearerTokenFile(file) =>
            file.unwrap.getAbsolutePath shouldBe tokenFile.toJava.getAbsolutePath
          }
        }
      }
    }

    "remote sequencer ha onboarding config" in {
      val configE =
        CantonConfig.parseAndLoad(
          Seq(remoteSequencerHAConfig.toJava),
          Some(DefaultPorts.create()),
        )
      valueOrFail(configE)("loading remote sequencer ha config")
    }

    "distributed topology produce a port definition message" in {
      val configE = CantonConfig.parseAndLoad(
        Seq(distributedConf.toJava),
        Some(DefaultPorts.create()),
      )
      val config = valueOrFail(configE)("loading distributed synchronizer topology config")

      val expectedValues = Set(
        "participant1:admin-api=5012,ledger-api=5011",
        "participant2:admin-api=5022,ledger-api=5021",
        "participant3:admin-api=5032,ledger-api=5031",
        "sequencer1:admin-api=5002,public-api=5001",
        "mediator1:admin-api=6002",
      )

      config.portDescription.split(";").toSet shouldBe expectedValues
    }

    "parse all node identifier configs" in {
      forAll(
        Table[String, NodeIdentifierConfig](
          ("configName", "expected"),
          ("explicit-node-identifier.conf", NodeIdentifierConfig.Explicit("MyName")),
          ("random-node-identifier.conf", NodeIdentifierConfig.Random),
          ("config-node-identifier.conf", NodeIdentifierConfig.Config),
        )
      ) { case (configName, expectedConfig) =>
        val file = "community" / "app" / "src" / "test" / "resources" / configName
        val config = CantonConfig
          .parseAndLoad(
            Seq(simpleConf, file).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .value
        config
          .participantsByString("participant1")
          .init
          .identity should matchPattern { case IdentityConfig.Auto(`expectedConfig`) => }

      }
    }
  }

  "config validation on duplicate storage" should {
    "return a ValidationError during loading" in {
      val result = loggerFactory.assertLogs(
        CantonConfig.parseAndLoad(
          Seq(simpleConf.toJava, duplicateStorageWithoutReplicationConfig.toJava),
          Some(DefaultPorts.create()),
        ),
        _.errorMessage should (include("Failed to validate the configuration") and include(
          "participant1"
        ) and include("participant2")),
      )
      result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
    }

    "pass when replication flag is enabled" in {
      CantonConfig
        .parseAndLoad(
          Seq(simpleConf.toJava, duplicateStorageWithReplicationConfig.toJava),
          Some(DefaultPorts.create()),
        )
        .valueOrFail("loading config")
    }
  }

  "config validation on protocol version" should {
    "return an error during loading a config with a minimum protocol version not supported by the participant" in {
      val result = loggerFactory.assertLogs(
        CantonConfig.parseAndLoad(
          Seq(simpleConf.toJava, unsupportedMinProtocolVersionConfig.toJava),
          Some(DefaultPorts.create()),
        ),
        _.errorMessage should (include("unsupported-minimum-protocol-version.conf") and include(
          "42"
        ) and include(
          ProtocolVersionCompatibility
            .supportedProtocols(
              includeDevVersion = false,
              includeAlphaVersions = false,
              includeBetaVersions = false,
              release = ReleaseVersion.current,
            )
            .headOption
            .value
            .toString
        )),
      )
      val resVal = result.left.value

      resVal shouldBe a[ConfigErrors.GenericConfigError.Error]
      resVal.cause should (include("unsupported-minimum-protocol-version.conf") and include(
        "42"
      ))
    }
  }

  "performance test configs" should {
    Seq(
      "synchronizers.conf",
      "synchronizers-sec.conf",
      "participants.conf",
      "participants-sec.conf",
      "performance-runners.conf",
    ).foreach { file =>
      s"parse $file" in {
        CantonConfig
          .parseAndLoad(
            Seq(
              perf / "config-test.env",
              perf / "config" / "bft" / "_sequencer_type.conf",
              perf / "config" / "postgres" / "_persistence.conf",
              perf / "config" / file,
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .valueOrFail("loading config")
      }
    }
  }

  "deprecated configs" should {

    def deprecatedConfigChecks(config: CantonConfig) = {
      // verify that `http-ledger-api` is configured with the expected values
      val participant = config.participants.headOption.value._2
      participant.httpLedgerApi.address shouldBe "127.0.0.101"
      participant.httpLedgerApi.port.unwrap shouldBe 102
      participant.httpLedgerApi.portFile shouldBe Some(Path.of("103"))
      participant.httpLedgerApi.pathPrefix shouldBe Some("104")
      participant.httpLedgerApi.requestTimeout.toMinutes shouldBe 105

      // verify that `crypto.sessionSigningKeys` is configured with the expected values
      participant.crypto.sessionSigningKeys shouldBe SessionSigningKeysConfig.enabled

      // verify that `participant.trafficAccounting.enabled` is configured with the expected values
      participant.trafficAccounting.enabled shouldBe true
    }

    // In this test case, both deprecated and new fields are set with opposite values, we make sure the new fields
    // are used
    "load with new fields set" in {
      val deprecatedConfigLogs = Seq[(LogEntry => Assertion, String)](
        (
          _.message should (include("Config field") and include("is deprecated")),
          "moved field",
        )
      )
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig
            .parseAndLoad(
              Seq(
                env,
                confDir / "storage" / "postgres.conf",
                deprecatedConfigs / "new-config-fields-take-precedence.conf",
              )
                .map(
                  _.toJava
                ),
              Some(DefaultPorts.create()),
            )
            .value
          deprecatedConfigChecks(parsed)
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }

    // In this test case, only the deprecated fields are set, we make sure they get used as fallbacks
    "be backwards compatible" in {
      val deprecatedConfigLogs = Seq[(LogEntry => Assertion, String)](
        (
          _.message should include(
            "Config field at http-ledger-api.server is deprecated since 3.4.0"
          ),
          "deprecated field 'http-ledger-api.server'",
        ),
        (
          _.message should include(
            "Config field at kms.session-signing-keys is deprecated since 3.5.0"
          ),
          "deprecated field 'kms.session-signing-keys'",
        ),
        (
          _.message should include(
            "Config field at traffic-enforcement is deprecated since 3.6.0"
          ),
          "deprecated field 'traffic-enforcement'",
        ),
      )
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig
            .parseAndLoad(
              Seq(
                env,
                confDir / "storage" / "postgres.conf",
                deprecatedConfigs / "backwards-compatible.conf",
              ).map(
                _.toJava
              ),
              Some(DefaultPorts.create()),
            )
            .value
          deprecatedConfigChecks(parsed)
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }

    "tolerate and log removed fields" in {
      val deprecatedConfigLogs = Seq(
        "enable-deprecated-endpoints-34",
        "enable-deprecated-parameters-34",
        "enable-deprecated-endpoints-35",
      ).map { flag =>
        (
          (_: LogEntry).message should include(
            s"Config path 'features.deprecated.$flag' is deprecated since 3.7.0"
          ),
          s"deprecated path 'features.deprecated.$flag'",
        )
      }
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig.parseAndLoad(
            Seq(
              env,
              confDir / "storage" / "postgres.conf",
              deprecatedConfigs / "removed-fields.conf",
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          parsed.value.participants.keySet should contain(InstanceName.tryCreate("participant1"))
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }
  }
  "AuthServiceConfig parsing" should {
    "correctly parse max-token-life for all provider types" in {
      import com.digitalasset.canton.config.AuthServiceConfig.*
      import scala.concurrent.duration.*

      val authConfigStr =
        """
          |canton.participants.participant1.ledger-api.auth-services = [
          |  {
          |    type = "unsafe-jwt-hmac-256"
          |    secret = "super-secret-test-key-here"
          |    max-token-life = "10m"
          |  },
          |  {
          |    type = "jwt-rs-256-crt"
          |    certificate = "path/to/rsa-cert.crt"
          |    max-token-life = "20m"
          |  },
          |  {
          |    type = "jwt-jwks"
          |    url = "https://example.com/.well-known/jwks.json"
          |    max-token-life = "30m"
          |  },
          |  {
          |    type = "jwt-es-256-crt"
          |    certificate = "path/to/cert256.crt"
          |    max-token-life = "40m"
          |  },
          |  {
          |    type = "jwt-es-512-crt"
          |    certificate = "path/to/cert512.crt"
          |    max-token-life = "50m"
          |  }
          |]
          |""".stripMargin

      File.usingTemporaryFile("auth-services-test", ".conf") { tempFile =>
        tempFile.writeText(authConfigStr)

        val parsedConfig = CantonConfig
          .parseAndLoad(
            Seq(simpleConf.toJava, tempFile.toJava),
            Some(DefaultPorts.create()),
          )
          .valueOrFail("Failed to parse config with auth services")

        val authServices = parsedConfig.participantsByString("participant1").ledgerApi.authServices

        authServices should have size 5

        inside(authServices) {
          case Seq(
                unsafe: UnsafeJwtHmac256,
                rs256: JwtRs256Crt,
                jwks: JwtJwks,
                es256: JwtEs256Crt,
                es512: JwtEs512Crt,
              ) =>
            unsafe.maxTokenLife.duration shouldBe 10.minutes
            rs256.maxTokenLife.duration shouldBe 20.minutes
            jwks.maxTokenLife.duration shouldBe 30.minutes
            es256.maxTokenLife.duration shouldBe 40.minutes
            es512.maxTokenLife.duration shouldBe 50.minutes
        }
      }
    }
  }
}
