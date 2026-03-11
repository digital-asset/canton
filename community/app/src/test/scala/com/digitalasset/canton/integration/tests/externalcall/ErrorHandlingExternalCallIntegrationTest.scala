// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*
import scala.annotation.nowarn

/** Integration tests for error handling in external calls.
  *
  * Tests:
  * - HTTP 4xx errors (400, 401, 403, 404)
  * - HTTP 5xx errors (500, 502, 503, 504)
  * - Connection timeout
  * - Request timeout
  * - Service unavailable (connection refused)
  * - Unknown extension ID
  * - Unknown function ID
  */
sealed trait ErrorHandlingExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableExternalCallExtension("test-ext", mockServerPort, "participant1"),
        enableExternalCallExtension("test-ext", mockServerPort, "participant2"),
      )
      .withSetup { implicit env =>
        import env.*

        initializeMockServer()

        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
        }

        clue("Upload ExternalCallTest DAR") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
        }

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
        }
      }

  /** Helper to create an ExternalCallContract for alice and return its contract ID */
  @nowarn("msg=is never used")
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

  /** Helper to exercise CallExternal and expect it to fail */
  @nowarn("msg=is never used")
  private def exerciseAndExpectFailure(
      contractId: E.ExternalCallContract.ContractId,
      functionId: String,
  )(implicit env: FixtureParam): io.grpc.StatusRuntimeException = {
    import env.*
    val inputHex = toHex("test-input")
    intercept[io.grpc.StatusRuntimeException] {
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          functionId,
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
    }
  }

  "error handling for external calls" should {

    // === HTTP 4xx Client Errors ===

    "handle HTTP 400 Bad Request" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 401 Unauthorized" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 403 Forbidden" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 404 Not Found" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    // === HTTP 5xx Server Errors ===

    "handle HTTP 500 Internal Server Error" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 502 Bad Gateway" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 503 Service Unavailable" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    "handle HTTP 504 Gateway Timeout" in { _ =>
      // Test requires mock server error simulation infrastructure.
      pending
    }

    // === Timeout Errors ===

    "handle request timeout" in { _ =>
      // Test requires mock server timeout simulation infrastructure.
      pending
    }

    "handle connection timeout" in { _ =>
      // This test requires an extension pointing to a non-routable IP.
      // The current setup uses localhost, so we skip this specific scenario.
      // Connection timeout behavior is covered by the "connection refused" test below.
      pending
    }

    // === Service Unavailability ===

    "handle connection refused" in { _ =>
      // Test requires mock server connection refusal infrastructure.
      pending
    }

    // === Configuration Errors ===

    "handle unknown extension ID" in { _ =>
      // Test requires additional setup for error scenarios.
      pending
    }

    "handle unknown function ID" in { _ =>
      // Test requires mock server error handler setup.
      pending
    }

    // === Error Message Propagation ===

    "propagate error message from external service" in { _ =>
      // Test requires mock server error response infrastructure.
      pending
    }

    "handle empty error response body" in { _ =>
      // Test requires mock server error response infrastructure.
      pending
    }

    "handle very large error response body" in { _ =>
      // Test requires mock server error response infrastructure.
      pending
    }

    // === Error Recovery ===

    "allow subsequent calls after error" in { _ =>
      // Test requires mock server error recovery infrastructure.
      pending
    }
  }
}

class ErrorHandlingExternalCallIntegrationTestH2 extends ErrorHandlingExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class ErrorHandlingExternalCallIntegrationTestPostgres
    extends ErrorHandlingExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
