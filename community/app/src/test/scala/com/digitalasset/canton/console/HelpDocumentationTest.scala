// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.environment.CommunityEnvironmentFixture
import com.digitalasset.canton.environment.CommunityEnvironmentFixture.captureStdout
import org.scalatest.wordspec.AnyWordSpec

/** Tests that the console command documentation adheres to defined restrictions, fails otherwise.
  */
class HelpDocumentationTest extends AnyWordSpec with CommunityEnvironmentFixture {

  "Help Documentation" should {
    "not trigger any formatting warnings (length, newlines, etc.)" in new TestEnvironment {

      private val p1Bootstrap = mockParticipant
      private val s1Bootstrap = mockSequencer
      private val m1Bootstrap = mockMediator

      setupParticipantFactory("p1", p1Bootstrap)
      setupSequencerFactory("s1", s1Bootstrap)
      setupMediatorFactory("m1", m1Bootstrap)

      private implicit val consoleEnv: ConsoleEnvironment = environment.createConsole()

      private val participant1 =
        consoleEnv.participants.all.headOption.valueOrFail("participant1 not found")
      private val sequencer1 =
        consoleEnv.sequencers.all.headOption.valueOrFail("sequencer1 not found")
      private val mediator1 = consoleEnv.mediators.all.headOption.valueOrFail("mediator1 not found")

      private val capturedOutput = captureStdout {

        Help.flattenItemsForManual(consoleEnv.helpItems)

        Help.getItemsFlattenedForManual(participant1, Seq("Participant"))
        Help.getItemsFlattenedForManual(mediator1, Seq("Mediator"))
        Help.getItemsFlattenedForManual(sequencer1, Seq("Sequencer"))

        Help.getItemsFlattenedForManual(
          new ParticipantReferencesExtensions(Seq.empty),
          Seq("Multiple Participants"),
        )

        val localInstancesExt =
          new LocalInstancesExtensions.Impl(Seq.empty[LocalParticipantReference])
        Help.getItemsFlattenedForManual(
          localInstancesExt,
          Seq("Local Instances"),
        )
      }

      if (capturedOutput.contains("[DOC WARNING]")) {
        fail(s"Help documentation generation triggered formatting warnings:\n$capturedOutput")
      }
    }
  }
}
