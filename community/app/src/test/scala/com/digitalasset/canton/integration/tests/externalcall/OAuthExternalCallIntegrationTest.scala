// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Duration
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

trait OAuthExternalCallTestFiles {
  protected val tokenEndpointPath: String = "/oauth2/token"

  protected lazy val trustCollectionFile: Path =
    Paths.get(BaseTest.getResourcePath("tls/root-ca.crt"))

  protected lazy val oauthPrivateKeyFile: Path = {
    val pemPath = Paths.get(BaseTest.getResourcePath("tls/public-api.pem"))
    val pem = Files.readString(pemPath, StandardCharsets.US_ASCII)
    val encoded = pem.linesIterator
      .filterNot(line => line.startsWith("-----BEGIN") || line.startsWith("-----END"))
      .mkString
    val privateKeyBytes = new PKCS8EncodedKeySpec(Base64.getMimeDecoder.decode(encoded)).getEncoded
    val derFile = Files.createTempFile("external-call-oauth-client", ".der")
    Files.write(derFile, privateKeyBytes)
    derFile.toFile.deleteOnExit()
    derFile
  }
}

sealed trait OAuthExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

  override def environmentDefinition: EnvironmentDefinition =
    externalCallEnvironmentDefinition(EnvironmentDefinition.P2S1M1_Manual)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableOAuthExternalCallExtension(
          extensionId = "test-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "expiry-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "replay-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "malformed-token-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "retry-token-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "terminal-token-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
      .withSetup { implicit env =>
        import env.*

        mockServer = new MockExternalCallServer(
          port = mockServerPort,
          loggerFactory = loggerFactory,
          useTls = true,
          tokenEndpointPath = Some(tokenEndpointPath),
        )
        mockServer.start()

        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
        }

        clue("Upload ExternalCallTest DAR") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
        }

        clue("Enable party") {
          alice = participant1.parties.enable("alice")
        }
      }

  private def createExternalCallContract()(implicit env: FixtureParam) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.ExternalCallContract(
        alice.toProtoPrimitive,
        java.util.List.of(),
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id
  }

  "oauth external call integration" should {

    "execute over HTTPS and reuse the cached token across later business requests" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setTokenSuccessHandler(accessToken = "cached-token", expiresIn = 120L)
      setupEchoHandler()

      val firstContractId = createExternalCallContract()
      val secondContractId = createExternalCallContract()

      val firstExerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        firstContractId.exerciseCallExternal(
          "test-ext",
          "echo",
          "00000000",
          toHex("oauth-first"),
        ).commands.asScala.toSeq,
      )

      val secondExerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        secondContractId.exerciseCallExternal(
          "test-ext",
          "echo",
          "00000000",
          toHex("oauth-second"),
        ).commands.asScala.toSeq,
      )

      firstExerciseTx.getUpdateId should not be empty
      secondExerciseTx.getUpdateId should not be empty
      verifyTokenCallCount(1)
      verifyCallCount("echo", 4)
    }

    "reacquire the token on the next business request after local expiry" in { implicit env =>
      import env.*

      resetMockServer()
      val issuedTokenCount = new AtomicInteger(0)
      mockServer.setTokenHandler { _ =>
        val tokenNumber = issuedTokenCount.incrementAndGet()
        ExternalCallResponse(
          statusCode = 200,
          body =
            s"""{"access_token":"expiring-token-$tokenNumber","token_type":"Bearer","expires_in":1}"""
              .getBytes(StandardCharsets.UTF_8),
          headers = Map("Content-Type" -> "application/json"),
        )
      }
      setupEchoHandler()

      val firstContractId = createExternalCallContract()
      val secondContractId = createExternalCallContract()

      val firstExerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        firstContractId.exerciseCallExternal(
          "expiry-ext",
          "echo",
          "00000000",
          toHex("oauth-expiry-first"),
        ).commands.asScala.toSeq,
      )

      firstExerciseTx.getUpdateId should not be empty
      verifyTokenCallCount(1)

      env.environment.simClock.foreach(_.advance(Duration.ofSeconds(2)))

      val secondExerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        secondContractId.exerciseCallExternal(
          "expiry-ext",
          "echo",
          "00000000",
          toHex("oauth-expiry-second"),
        ).commands.asScala.toSeq,
      )

      secondExerciseTx.getUpdateId should not be empty
      verifyTokenCallCount(2)
      verifyCallCount("echo", 4)
    }

    "refresh once and replay the resource request after a single resource-server 401" in { implicit env =>
      import env.*

      resetMockServer()
      val issuedTokenCount = new AtomicInteger(0)
      val resourceCallCount = new AtomicInteger(0)
      mockServer.setTokenHandler { _ =>
        val tokenNumber = issuedTokenCount.incrementAndGet()
        ExternalCallResponse(
          statusCode = 200,
          body =
            s"""{"access_token":"replay-token-$tokenNumber","token_type":"Bearer","expires_in":120}"""
              .getBytes(StandardCharsets.UTF_8),
          headers = Map("Content-Type" -> "application/json"),
        )
      }
      mockServer.setHandler("echo") { req =>
        if (resourceCallCount.incrementAndGet() == 1) {
          ExternalCallResponse.error(401, "Unauthorized")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "replay-ext",
          "echo",
          "00000000",
          toHex("oauth-replay"),
        ).commands.asScala.toSeq,
      )

      exerciseTx.getUpdateId should not be empty
      verifyTokenCallCount(2)
      verifyCallCount("echo", 3)
    }

    "retry the token endpoint through the outer retry loop before reaching the resource server" in {
      implicit env =>
        import env.*

        resetMockServer()
        val tokenAttemptCount = new AtomicInteger(0)
        mockServer.setTokenHandler { _ =>
          if (tokenAttemptCount.incrementAndGet() == 1) {
            ExternalCallResponse(
              statusCode = 503,
              body = "Token endpoint temporarily unavailable".getBytes(StandardCharsets.UTF_8),
              headers = Map("Retry-After" -> "0"),
            )
          } else {
            ExternalCallResponse(
              statusCode = 200,
              body =
                """{"access_token":"retry-token","token_type":"Bearer","expires_in":120}"""
                  .getBytes(StandardCharsets.UTF_8),
              headers = Map("Content-Type" -> "application/json"),
            )
          }
        }
        setupEchoHandler()

        val contractId = createExternalCallContract()

        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "retry-token-ext",
            "echo",
            "00000000",
            toHex("oauth-token-retry"),
          ).commands.asScala.toSeq,
        )

        exerciseTx.getUpdateId should not be empty
        verifyTokenCallCount(2)
        verifyCallCount("echo", 2)
      }

    "surface malformed token responses as final 502 errors before any resource call" in {
      implicit env =>
        import env.*

        resetMockServer()
        mockServer.setTokenHandler { _ =>
          ExternalCallResponse(
            statusCode = 200,
            body = """{"access_token":"broken"""".getBytes(StandardCharsets.UTF_8),
            headers = Map("Content-Type" -> "application/json"),
          )
        }

        val contractId = createExternalCallContract()

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "malformed-token-ext",
              "echo",
              "00000000",
              toHex("oauth-malformed-token"),
            ).commands.asScala.toSeq,
          ),
          logEntries => {
            val renderedLogs = logEntries.map(_.toString).mkString("\n")
            renderedLogs should include("Malformed OAuth token response")
            renderedLogs should include("status=502")
          }
        )

        verifyTokenCallCount(3)
        verifyCallCount("echo", 0)
    }

    "surface terminal token-endpoint failures with the preserved HTTP status" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setTokenErrorHandler(403, "Forbidden by test token endpoint")

      val contractId = createExternalCallContract()

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "terminal-token-ext",
            "echo",
            "00000000",
            toHex("oauth-terminal-token"),
          ).commands.asScala.toSeq,
        ),
        logEntries => {
          val renderedLogs = logEntries.map(_.toString).mkString("\n")
          renderedLogs should include("status=403")
          renderedLogs should include("Forbidden by test token endpoint")
        }
      )

      verifyTokenCallCount(1)
      verifyCallCount("echo", 0)
    }
  }
}

class OAuthExternalCallIntegrationTestH2 extends OAuthExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class OAuthExternalCallIntegrationTestPostgres extends OAuthExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
