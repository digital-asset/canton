// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.data.OnboardingRestriction
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.SequencerPenaltyUtils.sequencerPenalty
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.InitialOnboardingError
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*
import org.slf4j.event.Level

import scala.concurrent.duration.*

sealed trait PermissionedSynchronizerTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with SecurityTestSuite {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1

  private def setRestriction(
      restriction: OnboardingRestriction
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    sequencer1.topology.synchronizer_parameters
      .propose_update(sequencer1.synchronizer_id, _.update(onboardingRestriction = restriction))
    eventually() {
      val params = sequencer1.topology.synchronizer_parameters
        .list(store = sequencer1.synchronizer_id)
      params.loneElement.item.onboardingRestriction
        .transformInto[OnboardingRestriction] shouldBe restriction
    }
  }

  private def tag(threat: String, mitigation: String): SecurityTest = SecurityTest(
    property = Authorization,
    asset = "synchronizer node",
    attack = Attack(
      actor = "participant operator",
      threat = threat,
      mitigation = mitigation,
    ),
  )

  private def attemptConnectButFail(
      participant: ParticipantReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    clue("attempt to connect to synchronizer without being on the allow list") {
      assertThrowsAndLogsCommandFailures(
        // connection will fail as the participant is not known to the synchronizer
        participant.synchronizers.reconnect(daName, retry = false),
        _.commandFailureMessage should include(InitialOnboardingError.id),
      )
    }
  }

  "participant must be on the allow list to join a restricted synchronizer" taggedAs tag(
    threat = "onboard to a restricted synchronizer",
    mitigation = "refuse onboarding unless participant is on the allow list",
  ) in { implicit env =>
    import env.*
    setRestriction(OnboardingRestriction.RestrictedOpen)

    def attemptRegistration(performHandshake: Boolean): Unit =
      participant1.synchronizers.register(
        sequencer1,
        alias = daName,
        performHandshake = performHandshake,
        manualConnect = true,
      )

    assertThrowsAndLogsCommandFailures(
      // registration with handshake fails as the participant is not known to the synchronizer
      attemptRegistration(performHandshake = true),
      _.commandFailureMessage should include(InitialOnboardingError.id),
    )

    // registration without handshake succeeds as it is a local operation
    attemptRegistration(performHandshake = false)

    attemptConnectButFail(participant1)

    setRestriction(OnboardingRestriction.RestrictedLocked)
    clue("synchronizer owner grants the right (but the synchronizer is locked now)") {
      sequencer1.topology.participant_synchronizer_permissions.propose(
        sequencer1.synchronizer_id,
        participant1.id,
        ParticipantPermission.Submission,
      )
    }
    attemptConnectButFail(participant1)

    setRestriction(OnboardingRestriction.RestrictedOpen)
    participant1.synchronizers.reconnect(daName) // should succeed
    participant1.health.ping(participant1)
    participant1.synchronizers.active(daName) shouldBe true
  }

  "continue to modify the topology state without breaking the connection" in { implicit env =>
    import env.*

    Seq(
      ParticipantPermission.Observation,
      ParticipantPermission.Confirmation,
      ParticipantPermission.Submission,
    ).foreach { permission =>
      // we just change the state here and expect no error to be thrown
      sequencer1.topology.participant_synchronizer_permissions.propose(
        sequencer1.synchronizer_id,
        participant1.id,
        permission,
      )

    }
  }

  "synchronizer can revoke the participant synchronizer permission" onlyRunLessThan ProtocolVersion.v38 in {
    implicit env =>
      import env.*

      val adminPartyBefore = sequencer1.parties.list(
        filterParty = participant1.id.adminParty.filterString,
        synchronizerIds = daId,
      )
      adminPartyBefore.loneElement.participants.exists(_.participant == participant1.id)

      val subscriptionPermissionDenied = "Closing sequencer subscription due to permission denied"
      // revoking the participant synchronizer permission results in the sequencer denying the participant's subscriptions.
      // The participant stays connected and keeps retrying, in case it is allowed back on the synchronizer.
      // this produces a bunch of warnings
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          sequencer1.topology.participant_synchronizer_permissions.revoke(
            sequencer1.synchronizer_id,
            participant1.id,
          )

          val adminPartyAfter =
            sequencer1.parties.list(
              filterParty = participant1.id.adminParty.filterString,
              synchronizerIds = daId,
            )
          adminPartyAfter.loneElement.participants.map(
            _.participant
          ) should not contain participant1.id

          eventually() {
            sequencerPenalty(
              participant1,
              sequencer1.synchronizer_id,
              sequencer1.id,
            ) should be > 0.0
          }

          // the participant recovers without any operator action: its retries succeed once the renewed permission is
          // effective. Until then, including while the permission is proposed, it may still be denied, and it may warn
          // about the missing permission.
          // In some specific case, where the sequencer terminates the connection before sending the
          // revocation transaction, no warning will be emitted.
          clue("allow the participant back on the synchronizer") {
            sequencer1.topology.participant_synchronizer_permissions.propose(
              sequencer1.synchronizer_id,
              participant1.id,
              ParticipantPermission.Submission,
            )
          }

          // Increase retry time to exceed ping timeout and the backoff of the connection restarts
          eventually(timeUntilSuccess = 45.seconds) {
            participant1.health.maybe_ping(participant1) should not be empty
          }
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.warningMessage should include(
                s"Unable to find ParticipantSynchronizerPermission for participant ${participant1.id} on synchronizer ${daId.logical}"
              ),
              "warn about missing participant synchronizer permission",
            ),
            (
              _.warningMessage should include(s"${participant1.id} access is disabled"),
              "all token expiry related errors",
            ),
            (
              _.warningMessage should include(subscriptionPermissionDenied),
              "subscription closed due to permission denied",
            ),
          ),
          mayContain = Seq(
            _.warningMessage should include("PERMISSION_DENIED/Authentication token refresh error"),
            _.warningMessage should include("Token refresh aborted due to shutdown"),
            // a ping submitted while the participant is still replaying past the revocation is
            // rejected until the re-granted permission becomes effective
            _.warningMessage should include("UNKNOWN_SUBMITTERS"),
          ),
        ),
      )
  }

  "synchronizer can change from restricted to unrestricted locked" in { implicit env =>
    import env.*
    setRestriction(OnboardingRestriction.UnrestrictedLocked)
    participant1.synchronizers.active(daName) shouldBe true
    participant1.health.ping(participant1)
  }

  "participant cannot join an unrestricted, but locked synchronizer" taggedAs tag(
    threat = "onboard to a unrestricted, but locked synchronizer",
    mitigation = "refuse onboarding",
  ) in { implicit env =>
    import env.*
    participant2.synchronizers.register(
      sequencer1,
      alias = daName,
      performHandshake = false,
      manualConnect = true,
    )
    attemptConnectButFail(participant2)
    participant2.synchronizers.active(daName) shouldBe false
    setRestriction(OnboardingRestriction.UnrestrictedOpen)

    participant2.synchronizers.reconnect(daName) // should succeed
    participant2.synchronizers.active(daName) shouldBe true
    participant2.health.ping(participant2)
  }

  "an unrestricted synchronizer can be locked again" taggedAs tag(
    threat = "onboard to a synchronizer which was locked down again",
    mitigation = "refuse onboarding",
  ) in { implicit env =>
    import env.*
    setRestriction(OnboardingRestriction.UnrestrictedLocked)

    participant3.synchronizers.register(
      sequencer1,
      alias = daName,
      performHandshake = false,
      manualConnect = true,
    )
    attemptConnectButFail(participant3)

    setRestriction(OnboardingRestriction.UnrestrictedOpen)

    participant3.synchronizers.reconnect(daName) // should succeed
    participant3.synchronizers.active(daName) shouldBe true
    participant3.health.ping(participant1)
  }

  "onboarded participants retain their rights when an unrestricted synchronizer becomes restricted" onlyRunWithOrGreaterThan ProtocolVersion.v38 in {
    implicit env =>
      import env.*

      participant3.synchronizers.disconnect(daName)
      setRestriction(OnboardingRestriction.RestrictedOpen)
      // participant3 is not on the allow list, but already onboarded.
      // Therefore, it can reconnect and transact normally
      participant3.synchronizers.reconnect(daName)

      participant3.health.ping(participant1)
  }
}

//class PermissionedSynchronizerTestDefault extends PermissionedSynchronizerTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}

final class PermissionedSynchronizerTestPostgres extends PermissionedSynchronizerTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
}
