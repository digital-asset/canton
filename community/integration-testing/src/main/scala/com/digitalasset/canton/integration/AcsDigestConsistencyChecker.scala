// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.SharedCantonConfig
import com.digitalasset.canton.console.{FeatureFlag, ParticipantReference}
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{
  AcsCommitmentConfig,
  ParticipantNodeConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.time.{Seconds, Span}

import scala.util.{Failure, Success, Try}

class AcsDigestConsistencyChecker(
    override protected val loggerFactory: NamedLoggerFactory,
    timeout: Timeout,
) extends AbstractChecker
    with NamedLogging
    with NoTracing
    with Eventually {

  private val interval = Interval(Span(2, Seconds))

  def verifyParticipantsAcsDigestConsistency(
      env: AnyTestConsoleEnvironment,
      plugins: Seq[BaseEnvironmentSetupPlugin[? <: SharedCantonConfig[?], ?]],
  ): Unit = {
    import env.*

    logger.info("Checking participant ACS digest consistency after tests...")

    val externalPluginO = plugins.collectFirst { case external: UseExternalProcess =>
      external
    }

    val externalParticipantNames = externalPluginO.map(_.externalParticipants).getOrElse(Set.empty)

    val localParticipants = participants.local.filter { participant =>
      participant.is_running &&
      participant.runningNode
        .exists(_.getNode.isDefined)
    }

    val externalParticipants =
      participants.remote.filter(participant => externalParticipantNames.contains(participant.name))

    val activeParticipants = (localParticipants ++ externalParticipants).filter { participant =>
      featureSet.contains(FeatureFlag.Testing) &&
      participant.health.is_running() &&
      participant.health.status.isActive
        .getOrElse(false)
    }

    val allParticipantAndSynchronizerPairs: Seq[(ParticipantReference, SynchronizerId)] =
      activeParticipants.flatMap { participant =>
        val config: AcsCommitmentConfig = participant.config match {
          case localParticipantConfig: ParticipantNodeConfig =>
            localParticipantConfig.parameters.acsCommitments
          case _: RemoteParticipantConfig =>
            val acsConfigO = for {
              externalPlugin <- externalPluginO
              cantonConfig <- externalPlugin.configs.get(participant.name)
              participantConfig <- cantonConfig.participants.get(participant.name)
            } yield participantConfig.parameters.acsCommitments

            acsConfigO.getOrElse(
              throw new RuntimeException(
                s"Participant config is not available for external participant ${participant.name}"
              )
            )
        }

        if (config.enableNewAcsCommitmentProcessor) {
          participant.health.status
          logger.info(
            s"Checking participant ACS digest consistency for participant ${participant.name}..."
          )
          val synchronizerIds = participant.synchronizers
            .list_registered()
            .flatMap(_._2.toOption)
            .map(_.logical)

          synchronizerIds.map { synchronizerId =>
            (participant, synchronizerId)
          }
        } else {
          List.empty
        }
      }

    // Run max 4 checks in parallel:
    //  - start all 4 checks
    //  - wait for all 4 to finish
    //  - take the next group if exists
    allParticipantAndSynchronizerPairs.grouped(4).foreach { participantAndSynchronizers =>
      participantAndSynchronizers.foreach { case (participant, synchronizerId) =>
        Try(
          timeIt(
            participant.commitments.run_digest_consistency_check(synchronizerId),
            s"kicking off ACS digest consistency for participant ${participant.name}",
          )
        ) match {
          case Success(_) =>
            logger.info(
              s"Kicking off ACS digest consistency for participant ${participant.name}... OK"
            )
          case Failure(t) =>
            logger.error(
              s"Kicking off ACS digest consistency for participant ${participant.name}... FAILED",
              t,
            )
        }
      }

      timeIt(
        eventually(timeout, interval) {
          val consistencyCheckStatusMap =
            participantAndSynchronizers.map { case (participant, synchronizerId) =>
              val isRunning =
                participant.commitments.digest_consistency_check_status(synchronizerId)

              (participant, synchronizerId) -> isRunning
            }.toMap

          val keysWithCheckStillRunning = consistencyCheckStatusMap.filter { case (_, status) =>
            status.isRunning
          }.keySet

          assert(
            keysWithCheckStillRunning.isEmpty,
            s"The consistency check should be finished eventually for all participant-synchronizer pairs. Unexpectedly still running for $keysWithCheckStillRunning",
          )
        },
        s"consistency checks for $participantAndSynchronizers participant-synchronizer pairs",
      )
    }

    logger.info(s"Checking participant ACS digest consistency... DONE")
  }
}
