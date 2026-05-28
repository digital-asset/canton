// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  ConfigIdentifier,
  Error,
  InconsistentLogicalSynchronizerIds,
  InconsistentSequencerIds,
  LsuOngoing,
  LsuSource,
  LsuTarget,
  MissingConfigForSynchronizer,
  SynchronizerIdAlreadyAdded,
  UnknownAlias,
  UnknownPsid,
}
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, Mutex}
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import monocle.macros.syntax.lens.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class InMemorySynchronizerConnectionConfigStore(
    val aliasResolution: SynchronizerAliasResolution,
    protected override val loggerFactory: NamedLoggerFactory,
) extends SynchronizerConnectionConfigStore
    with NamedLogging {

  protected implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)
  private val lock = new Mutex()

  private val configuredSynchronizerMap = TrieMap[
    (SynchronizerAlias, ConfiguredPhysicalSynchronizerId),
    StoredSynchronizerConnectionConfig,
  ]()

  private def getInternal(
      id: ConfigIdentifier
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig] = {
    def predicate(key: (SynchronizerAlias, ConfiguredPhysicalSynchronizerId)): Boolean = {
      val (entryAlias, entryConfiguredPsid) = key
      id match {
        case ConfigIdentifier.WithPsid(psid) => entryConfiguredPsid.toOption.contains(psid)
        case ConfigIdentifier.WithAlias(alias, configuredPsid) =>
          (alias, configuredPsid) == (entryAlias, entryConfiguredPsid)
      }
    }

    configuredSynchronizerMap
      .collectFirst { case (key, value) if predicate(key) => value }
      .toRight(MissingConfigForSynchronizer(id))
  }

  /** Performs the put. Note: should be guarded with the lock.
    */
  private def putInternal(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPsid: ConfiguredPhysicalSynchronizerId,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  ): Either[Error, Unit] =
    for {
      _ <- predecessorCompatibilityCheck(configuredPsid, synchronizerPredecessor)

      alias = config.synchronizerAlias

      /*
        Adding a new synchronizer with status Active during an LSU (e.g., register) can break LSU.
        It is most likely the result of an automation running in the background.
       */
      _ <-
        if (status == Active) checkNoLsuOngoing(config.synchronizerAlias)
        else ().asRight

      _ <- configuredPsid match {
        case KnownPhysicalSynchronizerId(psid) =>
          for {
            _ <- checkAliasConsistent(psid, alias)
            _ <- checkLogicalIdConsistent(psid, alias)
          } yield ()

        case UnknownPhysicalSynchronizerId => ().asRight
      }

      _ <- checkStatusConsistent(configuredPsid, alias, status)

      _ <- configuredSynchronizerMap
        .putIfAbsent(
          (config.synchronizerAlias, configuredPsid),
          StoredSynchronizerConnectionConfig(
            config,
            status,
            configuredPsid,
            synchronizerPredecessor,
          ),
        )
        .fold(Either.unit[ConfigAlreadyExists])(existingConfig =>
          Either.cond(
            config == existingConfig.config && synchronizerPredecessor == existingConfig.predecessor,
            (),
            ConfigAlreadyExists(config.synchronizerAlias, configuredPsid),
          )
        )
    } yield ()

  override def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPsid: ConfiguredPhysicalSynchronizerId,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] =
    EitherT.fromEither[FutureUnlessShutdown](lock.exclusive {
      putInternal(config, status, configuredPsid, synchronizerPredecessor)
    })

  /** Ensure no LSU is ongoing for the alias. An LSU is ongoing if there exists a config and
    * successor config with statuses LsuSource and LsuTarget respectively.
    */
  private def checkNoLsuOngoing(alias: SynchronizerAlias): Either[LsuOngoing, Unit] = {
    val configPerPsid = configuredSynchronizerMap.toSeq.collect {
      case ((`alias`, KnownPhysicalSynchronizerId(psid)), storedConfig)
          if storedConfig.status == LsuSource || storedConfig.status == LsuTarget =>
        psid -> (storedConfig.predecessor, storedConfig.status)
    }.toMap

    configPerPsid.toSeq.traverse_ {
      case (psid, (Some(predecessor), LsuTarget)) =>
        configPerPsid
          .get(predecessor.psid)
          .collect { case (_, LsuSource) => () }
          .fold(().asRight[LsuOngoing])(_ => LsuOngoing(predecessor.psid, psid).asLeft)

      case _ => Right(())
    }
  }

  // Ensure there is no other active configuration
  private def checkStatusConsistent(
      psid: ConfiguredPhysicalSynchronizerId,
      alias: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  ): Either[Error, Unit] =
    if (!status.isActive) Either.right(())
    else {
      val existingPsid = configuredSynchronizerMap.collectFirst {
        case ((`alias`, configuredPsid), config) if config.status == Active =>
          configuredPsid
      }
      existingPsid match {
        case Some(`psid`) | None => Either.right(())
        case Some(otherConfiguredPsid) =>
          Either.left(
            AtMostOnePhysicalActive(alias, Set(otherConfiguredPsid, psid)): Error
          )
      }
    }

  // Check that a new psid is consistent with stored IDs for that alias
  private def checkLogicalIdConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): Either[Error, Unit] = {
    val configuredPsidsForAlias = configuredSynchronizerMap.keySet.collect { case (`alias`, id) =>
      id
    }

    configuredPsidsForAlias
      .collectFirst {
        case KnownPhysicalSynchronizerId(existingPsid) if existingPsid.logical != psid.logical =>
          existingPsid
      }
      .map(existing =>
        InconsistentLogicalSynchronizerIds(
          alias = alias,
          newPsid = psid,
          existingPsid = existing,
        )
      )
      .toLeft(())
      .leftWiden[Error]
  }

  // Ensure this psid is not already registered with another alias
  private def checkAliasConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): Either[Error, Unit] =
    configuredSynchronizerMap.keySet
      .collectFirst {
        case (existingAlias, id)
            if id == KnownPhysicalSynchronizerId(psid) && existingAlias != alias =>
          SynchronizerIdAlreadyAdded(psid, existingAlias)
      }
      .toLeft(())

  override def replace(
      configuredPsid: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForSynchronizer, Unit] =
    EitherT.fromEither(
      replaceInternal(config.synchronizerAlias, configuredPsid, _.copy(config = config))
    )

  private def replaceInternal(
      alias: SynchronizerAlias,
      configuredPsid: ConfiguredPhysicalSynchronizerId,
      modifier: StoredSynchronizerConnectionConfig => StoredSynchronizerConnectionConfig,
  ): Either[MissingConfigForSynchronizer, Unit] =
    Either.cond(
      configuredSynchronizerMap
        .updateWith((alias, configuredPsid))(_.map(modifier))
        .isDefined,
      (),
      MissingConfigForSynchronizer(ConfigIdentifier.WithAlias(alias, configuredPsid)),
    )

  override def get(
      alias: SynchronizerAlias,
      configuredPsid: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap
      .get((alias, configuredPsid))
      .toRight(MissingConfigForSynchronizer(ConfigIdentifier.WithAlias(alias, configuredPsid)))

  override def get(
      psid: PhysicalSynchronizerId
  ): Either[UnknownPsid, StoredSynchronizerConnectionConfig] = {
    val id = KnownPhysicalSynchronizerId(psid)
    configuredSynchronizerMap
      .collectFirst { case ((_, `id`), config) => config }
      .toRight(UnknownPsid(psid))
  }

  override def getAll(): Seq[StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap.values.toSeq

  /** We have no cache, so this is a noop. */
  override def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  /** We have no cache, so this is a noop. */
  override def clearCache()(implicit traceContext: TraceContext): Unit = ()

  override def close(): Unit = ()

  override def getAllFor(
      alias: SynchronizerAlias
  ): Either[UnknownAlias, NonEmpty[Seq[StoredSynchronizerConnectionConfig]]] = {
    val connections = configuredSynchronizerMap.collect { case ((`alias`, _), config) =>
      config
    }.toSeq

    if (connections.nonEmpty) NonEmpty.from(connections).toRight(UnknownAlias(alias))
    else UnknownAlias(alias).asLeft
  }

  override protected def getAllForAliasInternal(alias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[StoredSynchronizerConnectionConfig]] =
    FutureUnlessShutdown.pure(getAllFor(alias).map(_.forgetNE).getOrElse(Nil))

  override def setStatus(
      alias: SynchronizerAlias,
      configuredPsid: ConfiguredPhysicalSynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {

    val res =
      lock.exclusive {
        for {
          // ensure that there is an existing config in the store
          _ <- get(alias, configuredPsid)
          // check that there isn't already a different active configuration
          _ <- checkStatusConsistent(configuredPsid, alias, status)
          _ <- replaceInternal(alias, configuredPsid, _.copy(status = status)).leftWiden[Error]
        } yield ()
      }

    EitherT.fromEither(res)
  }

  override def upsert(
      psid: PhysicalSynchronizerId,
      insert: (
          SynchronizerConnectionConfig,
          SynchronizerConnectionConfigStore.Status,
          Option[SynchronizerPredecessor],
      ),
      overrideSequencerConnections: Option[SequencerConnections],
      overridePredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, StoredSynchronizerConnectionConfig] = {
    val data = Map(
      "insert data" -> insert.toString,
      "overrideSequencerConnections" -> overrideSequencerConnections.toString,
      "overridePredecessor" -> overridePredecessor.toString,
    )
    logger.info(s"Upserting connection config for synchronizer $psid, with data $data")

    val res =
      lock.exclusive {
        getInternal(ConfigIdentifier.WithPsid(psid)) match {
          case Left(_: MissingConfigForSynchronizer) =>
            val (config, status, predecessor) = insert

            putInternal(config, status, KnownPhysicalSynchronizerId(psid), predecessor).map { _ =>
              StoredSynchronizerConnectionConfig(
                config,
                status,
                KnownPhysicalSynchronizerId(psid),
                predecessor,
              )
            }

          case Right(storedConfig) =>
            val updatedStoredConfig = storedConfig
              .focus(_.config.sequencerConnections)
              .modify(value => overrideSequencerConnections.getOrElse(value))
              .focus(_.predecessor)
              .modify(value => overridePredecessor.fold(value)(Some(_)))
              .focus(_.config.synchronizerId)
              .replace(Some(psid))

            configuredSynchronizerMap
              .put(
                (storedConfig.config.synchronizerAlias, KnownPhysicalSynchronizerId(psid)),
                updatedStoredConfig,
              )
              .discard

            Right(updatedStoredConfig)
        }
      }

    EitherT.fromEither[FutureUnlessShutdown](res)
  }

  override def setSequencerIds(
      psid: PhysicalSynchronizerId,
      sequencerIds: Map[SequencerAlias, SequencerId],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Error, Unit] = {
    val res =
      lock.exclusive {
        for {
          storedConfig <- getInternal(ConfigIdentifier.WithPsid(psid))

          updatedConnectionConfig = sequencerIds.foldLeft(storedConfig.config) {
            case (config, (alias, id)) =>
              config
                .focus(_.sequencerConnections)
                .modify(_.modify(alias, _.withSequencerId(id)))
          }

          mergedConnectionConfig <-
            storedConfig.config
              .subsumeMerge(updatedConnectionConfig)
              .leftMap[Error](
                InconsistentSequencerIds(psid, sequencerIds, _)
              )

          updatedStoredConfig = storedConfig.copy(config = mergedConnectionConfig)
        } yield configuredSynchronizerMap
          .put(
            (storedConfig.config.synchronizerAlias, KnownPhysicalSynchronizerId(psid)),
            updatedStoredConfig,
          )
          .discard
      }

    EitherT.fromEither[FutureUnlessShutdown](res)
  }

  override def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    /*
    Checks whether changes need to be applied to the DB.
    Fails if both (alias, None) (alias, physicalSynchronizerId) are unknown.
     */
    def changeNeeded(): Either[MissingConfigForSynchronizer, Boolean] = {
      val psidOld = get(alias, UnknownPhysicalSynchronizerId).map(_.configuredPsid)
      val psidNew =
        get(alias, KnownPhysicalSynchronizerId(psid)).map(_.configuredPsid)

      // Check that there exist one entry for this alias without psid or the change is already applied
      (psidOld, psidNew) match {
        case (Right(_), _) => Right(true)
        case (
              Left(_: MissingConfigForSynchronizer),
              Right(KnownPhysicalSynchronizerId(`psid`)),
            ) =>
          Right(false)
        case (Left(_: MissingConfigForSynchronizer), _) =>
          Left(
            MissingConfigForSynchronizer(
              ConfigIdentifier.WithAlias(alias, UnknownPhysicalSynchronizerId)
            )
          )
      }
    }

    def performChange(): Either[Error, Unit] =
      lock.exclusive {
        for {
          _ <- checkAliasConsistent(psid, alias)
          _ <- checkLogicalIdConsistent(psid, alias)

          // Check that there exist one entry for this alias without psid
          config <- get(alias, UnknownPhysicalSynchronizerId)

          _ <- predecessorCompatibilityCheck(
            KnownPhysicalSynchronizerId(psid),
            config.predecessor,
          )

        } yield {
          configuredSynchronizerMap.addOne(
            (
              (alias, KnownPhysicalSynchronizerId(psid)),
              config.copy(configuredPsid = KnownPhysicalSynchronizerId(psid)),
            )
          )
          configuredSynchronizerMap.remove((alias, UnknownPhysicalSynchronizerId)).discard

          ()
        }
      }

    for {
      isChangeNeeded <- EitherT.fromEither[FutureUnlessShutdown](changeNeeded()).leftWiden[Error]

      _ <-
        if (isChangeNeeded)
          EitherT.fromEither[FutureUnlessShutdown](performChange())
        else {
          logger.debug(
            s"Physical synchronizer id for $alias is already set to $psid"
          )
          EitherTUtil.unitUS[Error]
        }
    } yield ()
  }
}
