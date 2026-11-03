// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.syntax.parallel.*
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.ConfigTransforms.generateUniqueH2DatabaseName
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{BlockSequencerConfig, SequencerConfig}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  ReferenceSequencerDriver,
  ReferenceSequencerDriverFactory,
}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.nonempty.NonEmpty
import com.typesafe.config.{Config, ConfigFactory}
import monocle.macros.syntax.lens.*

import scala.concurrent.{Await, ExecutionContext}
import scala.reflect.ClassTag

/** @param sequencerGroups
  *   If sequencerGroups is defined, all the sequencers of the same set will share the same storage
  *   (which means they are part of the same synchronizer). All sequencers in the config that are
  *   not in a defined group will be placed in the same default group. If it is not defined, all
  *   sequencers will share the same storage by default and one synchronizer only is assumed. If
  *   in-memory storage is defined, sequencers sharing storage is not supported (each one is a
  *   different synchronizer).
  */
class UseReferenceBlockSequencer[StorageConfigT <: StorageConfig](
    override protected val loggerFactory: NamedLoggerFactory,
    sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    postgres: Option[UsePostgres] = None,
)(implicit c: ClassTag[StorageConfigT])
    extends EnvironmentSetupPlugin {

  private val driverSingleWordName: String = "reference"
  private val driverDescription: String = "Reference Block Sequencer"

  private implicit val pluginExecutionContext: ExecutionContext =
    Threading.newExecutionContext(
      loggerFactory.threadName + s"-$driverSingleWordName-sequencer-plugin-execution-context",
      noTracingLogger,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[canton] var pgPlugin: Option[UsePostgres] = postgres

  protected val driverFactory: ReferenceSequencerDriverFactory = new ReferenceSequencerDriverFactory

  private final def dbNameForGroup(group: Int): String = s"${driverSingleWordName}_db_$group"

  protected val dbNames: NonEmpty[List[String]] = sequencerGroups match {
    case SingleSynchronizer => NonEmpty(List, dbNameForGroup(0))
    case MultiSynchronizer(_) =>
      NonEmpty(
        List,
        dbNameForGroup(0), // db 0 is the default one
        (1 to sequencerGroups.numberOfSynchronizers).map(i => dbNameForGroup(i)).toList *,
      )
  }

  private def driverConfigs(
      config: CantonConfig,
      storageConfigs: Map[InstanceName, StorageConfigT],
  ): Map[InstanceName, SequencerConfig] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.forClass(loggerFactory, getClass)
    // Only the sequencers of this plugin: with multiple synchronizers, other sequencers may be
    // configured by another sequencer plugin
    config.sequencers.filter { case (name, _) => storageConfigs.contains(name) }.map {
      case (sequencerName, originalConfig) =>
        val driverConfig: ReferenceSequencerDriver.Config[StorageConfig] =
          ReferenceSequencerDriver.Config(
            storageConfigs.getOrElse(
              sequencerName,
              ErrorUtil.invalidState(s"Missing storage config for $sequencerName"),
            )
          )

        sequencerName ->
          (originalConfig.sequencer match {
            case external: SequencerConfig.External =>
              // Here we preserve any settings coming from the original config, i.e. from ConfigTransforms
              external.copy(
                config = driverConfig
              )

            case _ =>
              SequencerConfig.External(
                BlockSequencerConfig(),
                driverConfig,
              )
          })
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    c.runtimeClass match {
      case cl if cl == classOf[DbConfig.Postgres] =>
        pgPlugin =
          if (pgPlugin.nonEmpty) pgPlugin
          else {
            val postgresPlugin = new UsePostgres(loggerFactory)
            postgresPlugin.beforeTests()
            Some(postgresPlugin)
          }
        val postgresPlugin =
          pgPlugin.getOrElse(
            throw new IllegalStateException("Impossible code path: pgPlugin is set above")
          )
        Await.result(
          dbNames.forgetNE.parTraverse_(db => postgresPlugin.recreateDatabaseForNodeWithName(db)),
          config.parameters.timeouts.processing.io.duration,
        )
      case _ =>
    }

    // in H2 we need to make db name unique, but it has to be matching for all sequencers, so we cache it
    lazy val dbNamesH2: Map[String, String] = dbNames.forgetNE.map { dbName =>
      dbName -> generateUniqueH2DatabaseName(dbName)
    }.toMap

    def dbToStorageConfig(
        dbName: String,
        dbParametersConfig: DbParametersConfig,
        baseDbConfig: Config,
    ): StorageConfigT =
      c.runtimeClass match {
        case cl if cl == classOf[DbConfig.H2] =>
          val h2DbName = dbNamesH2.getOrElse(
            dbName,
            throw new IllegalStateException(
              s"Impossible code path: dbName $dbName not found in $dbNamesH2"
            ),
          )
          DbConfig
            .H2(
              DbBasicConfig("user", "pass", h2DbName, "", 0).toH2DbConfig
                .copy(parameters = dbParametersConfig)
                .config
            )
            .asInstanceOf[StorageConfigT]
        case cl if cl == classOf[DbConfig.Postgres] =>
          val postgresPlugin =
            pgPlugin.getOrElse(
              throw new IllegalStateException("Impossible code path: pgPlugin is set above")
            )
          postgresPlugin
            .generateDbConfig(dbName, dbParametersConfig, baseDbConfig)
            .asInstanceOf[StorageConfigT]
        case cl if cl == classOf[StorageConfig.Memory] =>
          StorageConfig.Memory(parameters = dbParametersConfig).asInstanceOf[StorageConfigT]
        case other =>
          // E.g. Nothing; we need to check and fail b/c the Scala compiler doesn't enforce
          //  passing the ClassTag-reified type parameter, if it's only used for a ClassTag implicit
          sys.error(
            s"The $driverDescription sequencer driver doesn't recognize storage type $other"
          )
      }

    val storageConfigMap: Map[InstanceName, StorageConfigT] = sequencerGroups match {
      case SingleSynchronizer =>
        config.sequencers.map { case (name, sequencerConfig) =>
          val dbParameters = sequencerConfig.storage.parameters
          (name, dbToStorageConfig(dbNames.head1, dbParameters, sequencerConfig.storage.config))
        }
      case MultiSynchronizer(groups) =>
        groups.zipWithIndex.flatMap { case (sequencers, i) =>
          val dbName = dbNameForGroup(i + 1)
          sequencers.map { name =>
            val (dbParameters, baseDbConfig) = config.sequencers
              .get(name)
              .map(s => (s.storage.parameters, s.storage.config))
              .getOrElse((DbParametersConfig(), ConfigFactory.empty()))

            (name, dbToStorageConfig(dbName, dbParameters, baseDbConfig))
          }
        }.toMap
    }

    val sequencersToConfig: Map[InstanceName, SequencerConfig] =
      driverConfigs(config, storageConfigMap)

    def mapSequencerConfigs(
        kv: (InstanceName, SequencerNodeConfig)
    ): (InstanceName, SequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          sequencersToConfig.get(name).fold(cfg)(cfg.focus(_.sequencer).replace(_)),
        )
    }

    config
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }

  override def afterTests(): Unit = pgPlugin.foreach(_.afterTests())

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    pgPlugin.foreach { postgresPlugin =>
      Await.result(
        postgresPlugin.dropDatabases(dbNames),
        config.parameters.timeouts.processing.io.duration,
      )
    }
}

object UseReferenceBlockSequencer {

  sealed trait SequencerSynchronizerGroups {
    def numberOfSynchronizers: Int
  }

  case object SingleSynchronizer extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = 1
  }

  final case class MultiSynchronizer(sequencerGroups: Seq[Set[InstanceName]])
      extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = sequencerGroups.size
  }

  object MultiSynchronizer {
    def tryCreate(sequencerGroups: Set[String]*): MultiSynchronizer =
      MultiSynchronizer(sequencerGroups.map(_.map(InstanceName.tryCreate)))
  }
}
