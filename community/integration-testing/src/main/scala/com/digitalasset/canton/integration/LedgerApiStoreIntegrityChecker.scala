// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.{SharedCantonConfig, StorageConfig}
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.{FutureInstances, MonadUtil}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class LedgerApiStoreIntegrityChecker(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging
    with NoTracing {
  private val timeout = 60.seconds

  private def timeIt[T](f: => T, message: String): T = {
    val start = System.currentTimeMillis()
    val result = f
    val end = System.currentTimeMillis()
    val elapsed = end - start
    logger.info(s"LedgerApiStoreIntegrityChecker: $message took: $elapsed ms")
    result
  }

  def verifyParticipantLapiIntegrity(
      env: AnyTestConsoleEnvironment,
      plugins: Seq[BaseEnvironmentSetupPlugin[? <: SharedCantonConfig[?], ?]],
  ): Unit = {
    import env.*

    // Encapsulates the check for 'participantsWithoutLapiVerification'
    // to avoid duplicating the if/else logic and the log message.
    def verifyOrBypass(participantName: String, participantLoggingName: String)(
        verificationLogic: => FutureUnlessShutdown[Unit]
    ): FutureUnlessShutdown[Unit] =
      if (environment.testingConfig.participantsWithoutLapiVerification(participantName)) {
        FutureUnlessShutdown.pure(
          logger.info(
            s"Checking participant Ledger API Store integrity, for $participantLoggingName bypassed as it is specified in testingConfig.participantsWithoutLapiVerification."
          )
        )
      } else {
        verificationLogic
      }

    def verifyAsync(
        participantLoggingName: String,
        storageConfig: StorageConfig,
    ): FutureUnlessShutdown[Unit] = {
      val overallStart = System.currentTimeMillis()
      LedgerApiStore
        .initialize(
          storageConfig = storageConfig,
          storage = None,
          ledgerParticipantId = LedgerParticipantId.assertFromString("fakeid"),
          ledgerApiDatabaseConnectionTimeout = LedgerApiServerConfig().databaseConnectionTimeout,
          ledgerApiPostgresDataSourceConfig = PostgresDataSourceConfig(),
          timeouts = environmentTimeouts,
          loggerFactory = loggerFactory,
          metrics = LedgerApiServerMetrics.ForTesting,
          onlyForTesting_DoNotInitializeInMemoryState =
            true, // otherwise we emmit error logs and keep a DBDispatcher open for empty DBs
        )
        .transformWith {
          case Failure(t) =>
            val elapsed = System.currentTimeMillis() - overallStart
            logger.error(
              s"Checking participant Ledger API Store integrity, for $participantLoggingName...FAILED TO ACCESS DATABASE, took: $elapsed ms",
              t.getMessage,
            )
            FutureUnlessShutdown.failed(t)
          case Success(store) =>
            val elapsed = System.currentTimeMillis() - overallStart
            logger.info(
              s"Checking participant Ledger API Store integrity, for $participantLoggingName...CONNECTION ESTABLISHED took $elapsed ms, proceeding with integrity check"
            )
            val verificationStart = System.currentTimeMillis()
            FutureUnlessShutdown
              .lift(store)
              .flatMap((st: LedgerApiStore) =>
                st.verifyIntegrity(failForEmptyDB =
                  false // sometimes configured participants are not even started once
                ).transformWith {
                  case Success(_) =>
                    val elapsed = System.currentTimeMillis() - verificationStart
                    logger.info(
                      s"Checking participant Ledger API Store integrity, for $participantLoggingName...VERIFICATION DONE took $elapsed ms"
                    )
                    st.close()
                    FutureUnlessShutdown.pure(())
                  case Failure(t) =>
                    val elapsed = System.currentTimeMillis() - verificationStart
                    logger.error(
                      s"Checking participant Ledger API Store integrity, for $participantLoggingName...VERIFICATION FAILED took $elapsed ms",
                      t,
                    )
                    st.close()
                    FutureUnlessShutdown.failed(t)
                }
              )
        }
    }

    def verifyOverStore(
        participantName: String,
        participantLoggingName: String,
        storageConfig: StorageConfig,
    ): FutureUnlessShutdown[Unit] =
      verifyOrBypass(participantName, participantLoggingName) {
        storageConfig match {
          case _: StorageConfig.Memory =>
            FutureUnlessShutdown.pure(
              logger.error(
                s"Checking participant Ledger API Store integrity, for $participantLoggingName is FAILED, as it is backed by an InMemory store. For safety please consider: 1) keep local in-memory participant running at the end of the test, 2) switch to persistent storage 3) or as a last resort exclude in-memory, not running participants explicitly via testingConfig.participantsWithoutLapiVerification."
              )
            )

          case nonInMemoryStorageConfig =>
            FutureUnlessShutdown
              .pure(
                logger.info(
                  s"Checking participant Ledger API Store integrity, for $participantLoggingName..."
                )
              )
              .flatMap(_ => verifyAsync(participantLoggingName, nonInMemoryStorageConfig))
        }
      }

    logger.info("Checking participant Ledger API Store integrity after tests...")
    val (runningAndActiveParticipants, notRunningParticipants) =
      participants.local.partition(participant =>
        featureSet.contains(FeatureFlag.Testing) &&
          participant.is_running &&
          participant.runningNode.exists(_.getNode.isDefined) &&
          participant.health.is_running() &&
          participant.health.status.isActive
            .getOrElse(false) // for passive replicas we need to go over store as well
      )

    def checkRunningParticipants =
      runningAndActiveParticipants.map { runningParticipant =>
        verifyOrBypass(
          participantName = runningParticipant.name,
          participantLoggingName = s"local running participant ${runningParticipant.name}",
        ) {
          FutureUnlessShutdown
            .pure {
              runningParticipant.health.status
              logger.info(
                s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}..."
              )
              Try(
                timeIt(
                  runningParticipant.testing.state_inspection.verifyLapiStoreIntegrity(),
                  s"integrity check for running local participant ${runningParticipant.name}",
                )
              ) match {
                case Success(_) =>
                  logger.info(
                    s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}...OK"
                  )

                case Failure(t) =>
                  logger.error(
                    s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}...FAILED",
                    t,
                  )
              }
            }
        }
          .failOnShutdownToAbortException("failed check of local running participant")
      }

    def checkNonRunningParticipants =
      notRunningParticipants.map { notRunningParticipant =>
        val storageConfig = actualConfig.participants
          .getOrElse(
            notRunningParticipant.name,
            throw new IllegalStateException(
              s"No configuration found for a not running participant $notRunningParticipant."
            ),
          )
          .storage

        verifyOverStore(
          participantName = notRunningParticipant.name,
          participantLoggingName = s"not running local participant ${notRunningParticipant.name}",
          storageConfig = storageConfig,
        )
          .failOnShutdownToAbortException("ledger api store initialization")
      }

    def checkExternalParticipants: Seq[Future[Unit]] =
      plugins.collectFirst { case external: UseExternalProcess => external } match {
        case Some(externalPlugin) =>
          externalPlugin.externalParticipants.map { remoteParticipant =>
            verifyOverStore(
              participantName = remoteParticipant,
              participantLoggingName = s"for remote participant $remoteParticipant",
              storageConfig = externalPlugin.storageConfig(remoteParticipant),
            ).failOnShutdownToAbortException("ledger api store initialization")
          }.toSeq

        case None =>
          val remoteNonExcludedParticipants =
            participants.remote
              .map(_.name)
              .filterNot(environment.testingConfig.participantsWithoutLapiVerification)
          if (remoteNonExcludedParticipants.nonEmpty)
            logger.error(
              s"Checking participant Ledger API Store integrity: external plugin not found, but there are remote participants [$remoteNonExcludedParticipants]. For safety please exclude remote, non-external participants explicitly via testingConfig.participantsWithoutLapiVerification"
            )
          Seq(Future.successful(()))
      }

    timeIt(
      Await.result(
        MonadUtil.parTraverseWithLimit(4)(
          checkRunningParticipants ++
            checkNonRunningParticipants ++
            checkExternalParticipants
        )(identity)(FutureInstances.parallelFuture),
        timeout,
      ),
      "all checks",
    )
    logger.info(s"Checking participant Ledger API Store integrity...DONE")

  }
}
