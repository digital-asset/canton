// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.foldable._
import cats.syntax.functor._
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.{DomainAliasManager, DomainRegistryError}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore, SequencedEventStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}

import scala.concurrent.{ExecutionContext, Future}

/** Factory for [[SyncDomainPersistentState]].
  * Tries to discover existing persistent states or create new ones
  * and checks consistency of domain parameters and unique contract key domains
  */
class SyncDomainPersistentStateFactory(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantSettings: ParticipantSettingsLookup,
    storage: Storage,
    pureCryptoApi: CryptoPureApi,
    val indexedStringStore: IndexedStringStore,
    parameters: ParticipantNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Creates [[SyncDomainPersistentState]]s for all known domain aliases
    * provided that the domain parameters and a sequencer offset are known.
    * Does not check for unique contract key domain constraints.
    * Must not be called concurrently with itself or other methods of this class.
    */
  def initializePersistentStates(
      aliasManager: DomainAliasManager
  )(implicit traceContext: TraceContext): Future[Unit] = {

    def checkForDomainParameters(
        parameterStore: DomainParameterStore
    )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
      EitherT
        .fromOptionF(parameterStore.lastParameters, "No domain parameters in store")
        .void

    aliasManager.aliases.toList.traverse_ { alias =>
      val resultE = for {
        domainId <- EitherT.fromEither[Future](
          aliasManager.domainIdForAlias(alias).toRight("Unknown domain-id")
        )
        domainIdIndexed <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
        persistentState = createPersistentState(alias, domainIdIndexed)
        _lastProcessedPresent <- persistentState.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
          .leftMap(_ => "No persistent event")
        _ <- checkForDomainParameters(persistentState.parameterStore)
        _ = logger.debug(s"Discovered existing state for $alias")
      } yield syncDomainPersistentStateManager.put(persistentState)

      resultE.valueOr(error => logger.debug(s"No state for $alias discovered: ${error}"))
    }
  }

  /** Retrieves the [[SyncDomainPersistentState]] from the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]]
    * for the given domain if there is one. Otherwise creates a new [[SyncDomainPersistentState]] for the domain
    * and registers it with the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]].
    * Checks that the [[com.digitalasset.canton.protocol.StaticDomainParameters]] are the same as what has been persisted (if so)
    * and enforces the unique contract key domain constraints.
    *
    * Must not be called concurrently with itself or other methods of this class.
    */
  def lookupOrCreatePersistentState(
      domainAlias: DomainAlias,
      domainId: IndexedDomain,
      domainParameters: StaticDomainParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SyncDomainPersistentState] = {
    val persistentState = createPersistentState(domainAlias, domainId)
    for {
      _ <- checkAndUpdateDomainParameters(
        domainAlias,
        persistentState.parameterStore,
        domainParameters,
      )
      _ <- checkUniqueContractKeys(
        domainAlias,
        domainParameters.uniqueContractKeys,
        domainId.domainId,
      )
    } yield {
      syncDomainPersistentStateManager.putIfAbsent(persistentState)
      persistentState
    }
  }

  private def createPersistentState(
      alias: DomainAlias,
      domainId: IndexedDomain,
  ): SyncDomainPersistentState =
    syncDomainPersistentStateManager
      .get(domainId.item)
      .getOrElse(
        SyncDomainPersistentState(
          storage,
          alias,
          domainId,
          pureCryptoApi,
          parameters.stores,
          parameters.cachingConfigs,
          parameters.processingTimeouts,
          parameters.enableAdditionalConsistencyChecks,
          indexedStringStore,
          loggerFactory,
        )
      )

  private def checkAndUpdateDomainParameters(
      alias: DomainAlias,
      parameterStore: DomainParameterStore,
      newParameters: StaticDomainParameters,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainRegistryError, Unit] = {
    for {
      oldParametersO <- EitherT.liftF(parameterStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing domain parameters for domain $alias: $newParameters")
          EitherT.liftF[Future, DomainRegistryError, Unit](
            parameterStore.setParameters(newParameters)
          )
        case Some(oldParameters) =>
          // We exclude protocolVersion from the equality check
          val oldParametersForComparison =
            oldParameters.copy(protocolVersion = newParameters.protocolVersion)

          EitherT.cond[Future](
            oldParametersForComparison == newParameters,
            (),
            DomainRegistryError.ConfigurationErrors.DomainParametersChanged
              .Error(oldParametersO, newParameters): DomainRegistryError,
          )
      }
    } yield ()
  }

  private def checkUniqueContractKeys(
      domainAlias: DomainAlias,
      connectToUniqueContractKeys: Boolean,
      domainId: DomainId,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainRegistryError, Unit] = {
    val uckMode = participantSettings.settings.uniqueContractKeys
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalStateException("unique-contract-keys setting is undefined")
        )
      )
    for {
      _ <- EitherTUtil.condUnitET[Future](
        uckMode == connectToUniqueContractKeys,
        DomainRegistryError.ConfigurationErrors.IncompatibleUniqueContractKeysMode.Error(
          s"Cannot connect to domain ${domainAlias.unwrap} with${if (!connectToUniqueContractKeys) "out"
          else ""} unique contract keys semantics as the participant has set unique-contract-keys=$uckMode"
        ),
      )
      _ <- EitherT.cond[Future](!uckMode, (), ()).leftFlatMap { _ =>
        // If we're connecting to a UCK domain,
        // make sure that we haven't been connected to a different domain before
        val allDomains = syncDomainPersistentStateManager.getAll.keySet
        EitherTUtil.condUnitET[Future](
          allDomains.forall(_ == domainId),
          DomainRegistryError.ConfigurationErrors.IncompatibleUniqueContractKeysMode.Error(
            s"Cannot connect to domain ${domainAlias.unwrap} as the participant has UCK semantics enabled and has already been connected to other domains: ${allDomains
              .mkString(", ")}"
          ): DomainRegistryError,
        )
      }
    } yield ()
  }
}
