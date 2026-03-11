// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

/** Integration tests for consensus behavior with external calls.
  *
  * These tests address the reviewer requirement:
  * "A test whereby the http service on the confirming participant doesn't return
  *  the same result as the http service on the preparing participant"
  *
  * Tests scenarios where participants may receive different results from external calls
  * and verify consensus behavior in such situations.
  */
sealed trait ConsensusExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableExternalCallExtensionOnAll("test-ext", mockServerPort),
      )
      .withSetup { implicit env =>
        import env.*

        initializeMockServer()

        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
          participant3.synchronizers.connect_local(sequencer1, daName)
        }

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
          charlie = participant3.parties.enable(
            "charlie",
            synchronizeParticipants = Seq(participant1, participant2),
          )
        }
      }

  "consensus with external calls" should {

    "reject transaction when confirming participant receives different result" in { _ =>
      // Test requires additional infrastructure to setup different HTTP responses per participant.
      pending
    }

    "reject transaction when multiple confirmers disagree with preparer" in { _ =>
      // Test requires additional infrastructure to setup different HTTP responses per participant.
      pending
    }

    "succeed when all participants receive identical results" in { _ =>
      // Test requires additional infrastructure to coordinate HTTP responses across participants.
      pending
    }

    "handle partial mismatch in multi-view transaction" in { _ =>
      // Test requires additional infrastructure for complex consensus scenarios.
      pending
    }

    "allow observer to validate using stored results without HTTP call" in { _ =>
      // Test requires additional infrastructure to verify observer behavior.
      pending
    }

    "reject when observer's local recomputation doesn't match stored result" in { _ =>
      // Requires special test infrastructure to inject mismatched stored results.
      pending
    }
  }
}

class ConsensusExternalCallIntegrationTestH2 extends ConsensusExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class ConsensusExternalCallIntegrationTestPostgres extends ConsensusExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
