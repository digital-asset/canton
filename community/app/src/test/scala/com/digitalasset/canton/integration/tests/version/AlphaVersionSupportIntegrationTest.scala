// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInconsistentConnectivity
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion.AlphaProtocolVersion

/** This test covers:
  *   - the ability of participant with/without alpha version to connect to synchronizer
  *     with/without alpha version
  *
  * This test only gets run if at least one alpha version exists
  */
sealed trait AlphaVersionSupportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with FlagCloseable
    with HasCloseContext {

  protected val alphaVersionO: Option[AlphaProtocolVersion] = ProtocolVersion.alpha.headOption

  protected val sequencerGroups: MultiSynchronizer =
    DevOrAlphaSupportTestEnvironment.sequencerGroups

  override lazy val environmentDefinition: EnvironmentDefinition =
    DevOrAlphaSupportTestEnvironment.createEnvironment(
      testedProtocolVersion = testedProtocolVersion,
      versionSupport = DevOrAlphaSupportTestEnvironment.AlphaVersionSupport,
      synchronizer1ProtocolVersion = getAlphaVersion,
    )

  protected def getAlphaVersion: AlphaProtocolVersion =
    alphaVersionO.getOrElse(throw new IllegalStateException("There is no alpha version"))

  // If testedProtocolVersion is alpha, then the participants are also using an alpha protocol version. In that case, this test is wrong.
  "participant without alpha protocol version" should {
    "not be able to connect to a synchronizer with alpha protocol version" onlyRunWhen (!_.isAlpha && alphaVersionO.isDefined) in {
      env =>
        assertThrowsAndLogsCommandFailures(
          env.participant2.synchronizers.connect(env.sequencer1, env.daName),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(
              s"The protocol version required by the server ($getAlphaVersion) is not among the supported protocol versions by the client"
            )
          },
        )
    }

    "succeed subsequently on synchronizer without alpha protocol version" onlyRunWhen (!_.isAlpha && alphaVersionO.isDefined) in {
      env =>
        env.participant2.synchronizers
          .connect_local(env.sequencer2, alias = env.acmeName)
    }
  }

  "participant with alpha protocol version" should {
    "connect to a synchronizer with alpha protocol version" onlyRunWhen (alphaVersionO.isDefined) in {
      env =>
        env.participant1.synchronizers
          .connect_local(env.sequencer1, alias = env.daName)
        env.participant1.health.ping(env.participant1.id)
    }

    "also succeed on synchronizer without alpha protocol version" onlyRunWhen (alphaVersionO.isDefined) in {
      env =>
        import env.*

        participant1.synchronizers.connect(sequencer2, acmeName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant2.health.ping(participant1.id)
    }
  }
}

class AlphaVersionSupportIntegrationTestH2 extends AlphaVersionSupportIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}

class AlphaVersionSupportIntegrationTestPostgres extends AlphaVersionSupportIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}
