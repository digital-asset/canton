// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import monocle.macros.syntax.lens.*

import java.time.Duration

/** A set of vals and methods related to environment to avoid code duplication in:
  *   - [[AlphaVersionSupportIntegrationTest]]
  *   - [[DevVersionSupportIntegrationTest]]
  */
object DevOrAlphaSupportTestEnvironment {

  val sequencerGroups: MultiSynchronizer = MultiSynchronizer(
    Seq(
      Set(InstanceName.tryCreate("sequencer1")),
      Set(InstanceName.tryCreate("sequencer2")),
    )
  )

  sealed trait VersionSupport
  case object DevVersionSupport extends VersionSupport
  case object AlphaVersionSupport extends VersionSupport

  /** This method creates an environment with:
    *   - synchronizer1 (sequencer1, mediator1): dev/alpha support enabled (alias: da)
    *   - synchronizer2 (sequencer2, mediator2): dev/alpha support not enabled (alias: acme)
    *   - participant1 and participant2
    *
    * @param testedProtocolVersion
    *   [[com.digitalasset.canton.TestEssentials#testedProtocolVersion()]] from the test class
    * @param versionSupport
    *   Either dev or alpha
    * @param synchronizer1ProtocolVersion
    *   Protocol version for the synchronizer1, it should be:
    *   - dev for dev support testing
    *   - one of the alpha versions for alpha support testing
    */
  def createEnvironment(
      testedProtocolVersion: ProtocolVersion,
      versionSupport: VersionSupport,
      synchronizer1ProtocolVersion: ProtocolVersion,
  ): EnvironmentDefinition = {
    val minimumStableProtocolVersion =
      if (testedProtocolVersion.isDev || testedProtocolVersion.isAlpha)
        ProtocolVersion.latest
      else testedProtocolVersion

    val versionSupportUpdates = versionSupport match {
      case DevVersionSupport =>
        ConfigTransforms.enableParticipantsDevVersionSupport("participant1")
          ++ ConfigTransforms.enableSequencersDevVersionSupport("sequencer1")
          ++ ConfigTransforms.enableMediatorsDevVersionSupport("mediator1")
      case AlphaVersionSupport =>
        ConfigTransforms.enableParticipantsAlphaVersionSupport("participant1")
          ++ ConfigTransforms.enableSequencersAlphaVersionSupport("sequencer1")
          ++ ConfigTransforms.enableMediatorsAlphaVersionSupport("mediator1")
    }

    EnvironmentDefinition.P2S2M2_Config
      .addConfigTransform(ConfigTransforms.enableNonStandardConfig)
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") {
        // set on `participant1` a minimum-pv and enable alpha version support
        _.focus(_.parameters.minimumProtocolVersion)
          .replace(Some(ParticipantProtocolVersion(minimumStableProtocolVersion)))
          .focus(_.sequencerClient.warnDisconnectDelay)
          .replace(config.NonNegativeFiniteDuration(Duration.ofSeconds(30)))
      })
      .addConfigTransforms(versionSupportUpdates*)
      .withSetup { implicit env =>
        import env.*

        bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaults(
            cryptoConfig = sequencer1.config.crypto,
            protocolVersion = synchronizer1ProtocolVersion,
          ),
        )

        bootstrap.synchronizer(
          acmeName.unwrap,
          sequencers = Seq(sequencer2),
          mediators = Seq(mediator2),
          synchronizerOwners = Seq(sequencer2),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
      }
  }
}
