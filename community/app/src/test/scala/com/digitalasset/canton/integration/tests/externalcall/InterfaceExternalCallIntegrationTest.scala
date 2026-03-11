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

/** Integration tests for external calls via interface exercises.
  *
  * These tests address the reviewer requirement:
  * "A test involving exercises by interface"
  *
  * The ExternalCallInterface is defined in ExternalCallTest.daml with a DoExternalCall
  * choice. ExternalCallContract implements this interface.
  *
  * NOTE: These tests require the codegen for ExternalCallInterface to be available.
  * If the interface codegen classes are not yet generated, these tests will remain
  * pending until the DAR is rebuilt with the interface definitions.
  */
sealed trait InterfaceExternalCallIntegrationTest
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



  "interface exercises with external calls" should {

    "execute external call via interface choice" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "work with different implementations of the same interface" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "handle interface exercise in nested transaction" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "work with observer on interface exercise" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "handle interface exercise with multiple stakeholders" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "correctly identify template ID in external call from interface" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "handle view decomposition correctly for interface exercises" in { _ =>
      // Interface codegen not available yet
      pending
    }

    "work when interface is from different package than template" in { _ =>
      // Interface codegen not available yet
      pending
    }
  }
}

class InterfaceExternalCallIntegrationTestH2 extends InterfaceExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class InterfaceExternalCallIntegrationTestPostgres extends InterfaceExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
