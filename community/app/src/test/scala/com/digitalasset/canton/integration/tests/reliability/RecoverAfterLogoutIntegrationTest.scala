// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.digitalasset.canton.config
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry

/** Ensure a node can recover transparently after being logged out from the sequencer without
  * knowing it
  */
final class RecoverAfterLogoutIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, "da")
      }

  "A participant" should {
    "transparently recover from a logout" in { implicit env =>
      import env.*

      // Ping to make sure everything is ok
      participant1.health.ping(participant1)

      // Generate a token for p1 and then logout, this in fact logs out the member and invalidates all its tokens
      val token = sequencer1.authentication.generate_authentication_token(participant1)
      sequencer1.authentication.logout(token.token)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        eventually() {
          participant1.health
            // maybe_ping because the first ping will fail as the first send to the sequencer will be bounced back
            // because the token is invalid. The participant will automatically reconnect and recover its connection
            // to the sequencer however.
            .maybe_ping(
              participant1,
              timeout = config.NonNegativeFiniteDuration.ofSeconds(2).asFiniteApproximation,
            )
            .isDefined shouldBe true
        },
        LogEntry.assertLogSeq(
          Seq.empty,
          Seq(
            // We'll get warnings on the first send because the participant's token is invalid
            _.warningMessage should include("Request failed for server-sequencer1-0"),
            _.warningMessage should include("Failed to submit transaction"),
          ),
        ),
      )

    }
  }
}
