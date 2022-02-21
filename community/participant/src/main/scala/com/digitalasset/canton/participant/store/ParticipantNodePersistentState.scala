// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import akka.stream.Materializer
import cats.syntax.foldable._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateLookup
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.retry.RetryEither
import io.functionmeta.functionFullName

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** Some of the state of a participant that is not tied to a domain and must survive restarts.
  * Does not cover topology stores (as they are also present for domain nodes)
  * nor the [[RegisteredDomainsStore]] (for initialization reasons)
  */
class ParticipantNodePersistentState private (
    val settingsStore: ParticipantSettingsStore,
    val participantEventLog: ParticipantEventLog,
    val multiDomainEventLog: MultiDomainEventLog,
    val inFlightSubmissionStore: InFlightSubmissionStore,
    val commandDeduplicationStore: CommandDeduplicationStore,
    val pruningStore: ParticipantPruningStore,
    override protected val loggerFactory: NamedLoggerFactory,
) extends AutoCloseable
    with NamedLogging {
  override def close(): Unit =
    Lifecycle.close(multiDomainEventLog)(logger)
}

object ParticipantNodePersistentState {

  /** Creates a [[ParticipantNodePersistentState]] and initializes the settings store.
    *
    * @param uniqueContractKeysO If [[scala.Some$]], try to set the unique contract key mode accordingly,
    *                            but if the participant has previously been connected to a domain,
    *                            the domain's parameter for unique contract keys takes precedence.
    *                            If [[scala.None$]], skip storing and checking the UCK setting.
    */
  def apply(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      storage: Storage,
      clock: Clock,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      uniqueContractKeysO: Option[Boolean],
      parameters: ParticipantStoreConfig,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
  ): Future[ParticipantNodePersistentState] = {
    val settingsStore = ParticipantSettingsStore(storage, loggerFactory)
    val participantEventLog = ParticipantEventLog(storage, indexedStringStore, loggerFactory)
    val inFlightSubmissionStore = InFlightSubmissionStore(
      storage,
      parameters.maxItemsInSqlClause,
      parameters.dbBatchAggregationConfig,
      loggerFactory,
    )
    val commandDeduplicationStore = CommandDeduplicationStore(storage, loggerFactory)
    val pruningStore = ParticipantPruningStore(storage, loggerFactory)

    val logger = loggerFactory.getTracedLogger(ParticipantNodePersistentState.getClass)
    implicit lazy val loggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)

    def waitForSettingsStoreUpdate[A](
        lens: ParticipantSettingsStore.Settings => Option[A],
        settingName: String,
    ): Future[A] = {
      RetryEither(
        timeouts.activeInit.retries(50.millis),
        50.millis.toMillis,
        functionFullName,
        logger,
      ) {
        settingsStore.refreshCache()
        lens(settingsStore.settings).toRight(())
      } match {
        case Left(_) =>
          ErrorUtil.internalErrorAsync(
            new IllegalStateException(
              s"Passive replica failed to read $settingName, needs to be written by active replica"
            )
          )
        case Right(a) => Future.successful(a)
      }
    }

    def checkOrSetMaxDedupDuration(
        maxDeduplicationDuration: NonNegativeFiniteDuration
    ): Future[Unit] = {

      def checkStoredMaxDedupDuration(
          storedMaxDeduplication: NonNegativeFiniteDuration
      ): Future[Unit] = {
        if (maxDeduplicationDuration != storedMaxDeduplication) {
          logger.warn(
            show"Using the max deduplication duration ${storedMaxDeduplication} instead of the configured $maxDeduplicationDuration."
          )
        }
        Future.unit
      }

      if (storage.isActive) {
        settingsStore.settings.maxDeduplicationDuration match {
          case None => settingsStore.insertMaxDeduplicationDuration(maxDeduplicationDuration)
          case Some(storedMaxDeduplication) => checkStoredMaxDedupDuration(storedMaxDeduplication)
        }
      } else {
        // On the passive replica wait for the max deduplication duration to be written by the active replica
        waitForSettingsStoreUpdate(_.maxDeduplicationDuration, "max deduplication duration")
          .flatMap(checkStoredMaxDedupDuration)
      }
    }

    def setUniqueContractKeysSetting(uniqueContractKeys: Boolean): Future[Unit] = {
      def checkStoredSetting(stored: Boolean): Future[Unit] = {
        if (uniqueContractKeys != stored) {
          logger.warn(
            show"Using unique-contract-keys=$stored instead of the configured $uniqueContractKeys.\nThis indicates that the participant was previously started with unique-contract-keys=$stored and then the configuration was changed."
          )
        }
        Future.unit
      }

      if (storage.isActive) {
        for {
          // Figure out whether the participant has previously been connected to a UCK or a non-UCK domain.
          ucksByDomain <- syncDomainPersistentStates.getAll.toSeq.traverseFilter {
            case (domainId, state) =>
              state.parameterStore.lastParameters.map(
                _.map(param => domainId -> param.uniqueContractKeys)
              )
          }
          (nonUckDomains, uckDomains) = ucksByDomain.partitionMap { case (domainId, isUck) =>
            Either.cond(isUck, domainId, domainId)
          }
          toBePersisted =
            if (uckDomains.nonEmpty) {
              if (!uniqueContractKeys) {
                logger.warn(
                  show"Ignoring participant config of not enforcing unique contract keys as the participant has been previously connected to domains with unique contract keys: $uckDomains"
                )
              }
              true
            } else if (nonUckDomains.nonEmpty) {
              if (uniqueContractKeys) {
                logger.warn(
                  show"Ignoring participant config of enforcing unique contract keys as the participant has been previously connected to domains without unique contract keys: $nonUckDomains"
                )
              }
              false
            } else uniqueContractKeys
          _ <- settingsStore.settings.uniqueContractKeys match {
            case None => settingsStore.insertUniqueContractKeysMode(toBePersisted)
            case Some(persisted) => checkStoredSetting(persisted)
          }
        } yield ()
      } else {
        waitForSettingsStoreUpdate(_.uniqueContractKeys, "unique contract key config").flatMap(
          checkStoredSetting
        )
      }
    }

    for {
      _ <- settingsStore.refreshCache()
      _ <- uniqueContractKeysO.traverse_(setUniqueContractKeysSetting)
      _ <- maxDeduplicationDurationO.traverse_(checkOrSetMaxDedupDuration)
      multiDomainEventLog <- MultiDomainEventLog(
        syncDomainPersistentStates,
        participantEventLog,
        storage,
        clock,
        metrics,
        indexedStringStore,
        timeouts,
        loggerFactory,
      )
    } yield {
      new ParticipantNodePersistentState(
        settingsStore,
        participantEventLog,
        multiDomainEventLog,
        inFlightSubmissionStore,
        commandDeduplicationStore,
        pruningStore,
        loggerFactory,
      )
    }
  }
}
