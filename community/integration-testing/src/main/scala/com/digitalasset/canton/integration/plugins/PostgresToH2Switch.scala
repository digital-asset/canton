// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.{CantonConfig, DbConfig, SharedCantonConfig}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.BaseEnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy
import com.digitalasset.canton.logging.NamedLoggerFactory

import java.sql.DriverManager
import scala.util.Try
import scala.util.control.NonFatal

/** [TEST ONLY] CI experiment (PV37 lane), not for merging.
  *
  * When the JVM system property `canton-test.integration.postgres-to-h2=true` is set, an
  * integration test that registered a plain `new UsePostgres(loggerFactory)` gets
  * [[UseH2InsteadOfPostgres]] instead, unless the test needs Postgres. A test keeps Postgres if
  *   - it or one of its base classes is in [[keepOnPostgres]] (Postgres-only SQL, dump/restore,
  *     HA replication config, or it holds the `UsePostgres` instance and calls it);
  *   - it registers a plugin that only works with a shared, out-of-process database
  *     (`UseSharedStorage`, `UseExternalProcess`, `UseToxiproxy`);
  *   - its `UsePostgres` has a custom database layout (custom names, node-to-db mapping, connection
  *     limits, forced test container, or databases kept after the test).
  *
  * Unit tests that mix in `PostgresTest` are not affected.
  */
object PostgresToH2Switch {

  val PropertyName = "canton-test.integration.postgres-to-h2"

  lazy val enabled: Boolean = sys.props.get(PropertyName).exists(_.trim.equalsIgnoreCase("true"))

  /** Test classes or base classes/traits that stay on Postgres, with the reason. */
  val keepOnPostgres: Map[String, String] = Map(
    "com.digitalasset.canton.integration.tests.DbConfigIntegrationTest" ->
      "matches on DbConfig.Postgres and rewrites its connection config",
    "com.digitalasset.canton.integration.tests.PostgresReadOnlyIntegrationTest" ->
      "ALTER SYSTEM / pg_reload_conf on its own Postgres container",
    "com.digitalasset.canton.integration.tests.RecordReplayIntegrationTest" ->
      "PostgresDumpRestore on the UsePostgres instance",
    "com.digitalasset.canton.integration.tests.acs.LargeAcsIntegrationTestBase" ->
      "PostgresDumpRestore on the UsePostgres instance",
    "com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission.BenchtoolDumpRestoreITSpec" ->
      "dumpAllDatabases / restoreAllDatabases on the UsePostgres instance",
    "com.digitalasset.canton.integration.tests.ParticipantActiveTransitionStartupIntegrationTest" ->
      "HA replication config (Postgres only)",
    "com.digitalasset.canton.integration.tests.MediatorActiveTransitionStartupTestSetup" ->
      "HA replication config (Postgres only)",
    "com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiParticipantPruningTest" ->
      "LOCK TABLE ... IN ACCESS SHARE MODE (pg_dump-style locks)",
    "com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiDbTimeoutIntegrationTest" ->
      "LOCK TABLE ... IN ACCESS EXCLUSIVE MODE",
    "com.digitalasset.canton.integration.tests.traffic.ParticipantTrafficEnforcementDegradationTest" ->
      "SET LOCAL lock_timeout / LOCK TABLE ... IN ACCESS EXCLUSIVE MODE",
    "com.digitalasset.canton.integration.tests.operations.ViewConsistencyTest" ->
      "information_schema query with array_agg over the public and debug schemas",
    // Failed on H2 and passed on Postgres locally: the setup is sized so that ACHS initialization is still
    // running on Postgres when the test shuts the participant down. On in-memory H2 it has finished by then.
    "com.digitalasset.canton.integration.tests.ledgerapi.AchsInitIntegrationTest" ->
      "relies on ACHS initialization being slow enough to interrupt (sized for Postgres)",
  )

  private def isPostgresOnlyCompanion(plugin: AnyRef): Option[String] = plugin match {
    case _: UseSharedStorage[?] => Some("registers UseSharedStorage (replicas share one database)")
    case _: UseExternalProcess => Some("registers UseExternalProcess (nodes in other JVMs)")
    case _: UseToxiproxy => Some("registers UseToxiproxy")
    case _ => None
  }

  private def classHierarchyNames(clazz: Class[?]): Set[String] = {
    def go(c: Class[?]): Set[String] =
      Option(c).fold(Set.empty[String])(c =>
        Set(c.getName) ++ go(c.getSuperclass) ++ c.getInterfaces.toSet.flatMap(go)
      )
    go(clazz)
  }

  /** Why `suite` must keep Postgres, if it must. */
  def reasonToKeepPostgres(
      suite: AnyRef,
      plugins: Seq[AnyRef],
  ): Option[String] = {
    val names = classHierarchyNames(suite.getClass)
    keepOnPostgres
      .collectFirst { case (name, reason) if names.contains(name) => s"$name: $reason" }
      .orElse(plugins.collectFirst(Function.unlift(isPostgresOnlyCompanion)))
      .orElse(plugins.collectFirst {
        case pg: UsePostgres if !pg.usesDefaultDbLayout =>
          "UsePostgres with a custom database layout"
      })
  }

  /** Returns the plugins to use for `suite`, swapping `UsePostgres` for H2 where allowed. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def apply[C <: SharedCantonConfig[C], E <: Environment[C]](
      suite: AnyRef,
      plugins: Seq[BaseEnvironmentSetupPlugin[C, E]],
      loggerFactory: NamedLoggerFactory,
  ): Seq[BaseEnvironmentSetupPlugin[C, E]] =
    if (!enabled || !plugins.exists { case _: UsePostgres => true; case _ => false }) plugins
    else {
      val logger = loggerFactory.getLogger(getClass)
      val suiteName = suite.getClass.getName
      reasonToKeepPostgres(suite, plugins) match {
        case Some(reason) =>
          logger.info(s"$PropertyName: $suiteName keeps Postgres ($reason)")
          plugins
        case None =>
          logger.info(s"$PropertyName: $suiteName switched from Postgres to H2")
          plugins.map {
            case pg: UsePostgres =>
              // UsePostgres is an EnvironmentSetupPlugin, so C and E are CantonConfig and CantonEnvironment here.
              new UseH2InsteadOfPostgres(pg)
                .asInstanceOf[BaseEnvironmentSetupPlugin[C, E]]
            case other => other
          }
      }
    }
}

/** [[UseH2]] that also drops the in-memory H2 databases when the environment is destroyed, at the
  * point where [[UsePostgres]] drops its databases. Without this, every switched suite would leave
  * its databases in the test JVM heap (the URL sets `DB_CLOSE_DELAY=-1`) for the rest of the shard.
  *
  * Falls back to the original [[UsePostgres]] for the rest of the suite if a node in the config it
  * receives has replication enabled. Configs loaded from files that declare Postgres storage get
  * `replication.enabled = true` filled in at load time, and replicated storage needs database locks,
  * which H2 does not have: the node then fails to start, and a sequencer that fails to start exits
  * the whole test JVM (seen in CI with `ReferenceConfigExampleIntegrationTest`).
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class UseH2InsteadOfPostgres(original: UsePostgres) extends UseH2(original.loggerFactory) {

  private var fellBack = false

  private def nodesWithReplication(config: CantonConfig): Seq[String] =
    (config.participants.collect { case (n, c) if c.replication.exists(_.isEnabled) => n } ++
      config.mediators.collect { case (n, c) if c.replication.exists(_.isEnabled) => n } ++
      config.sequencers.collect { case (n, c) if c.replication.exists(_.isEnabled) => n })
      .map(_.unwrap)
      .toSeq

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = synchronized {
    if (!fellBack) {
      val replicated = nodesWithReplication(config)
      if (replicated.nonEmpty) {
        noTracingLogger.info(
          s"${PostgresToH2Switch.PropertyName}: falling back to Postgres, replication is enabled for ${replicated
              .mkString(", ")}"
        )
        original.beforeTests()
        fellBack = true
      }
    }
    if (fellBack) original.beforeEnvironmentCreated(config) else super.beforeEnvironmentCreated(config)
  }

  override def afterTests(): Unit = synchronized {
    if (fellBack) original.afterTests()
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    if (synchronized(fellBack)) original.afterEnvironmentDestroyed(config)
    else
      config.allLocalNodes.values.map(_.storage).foreach {
        case h2: DbConfig.H2 =>
          val c = h2.config
          val url = Try(c.getString("url")).getOrElse("")
          if (url.startsWith("jdbc:h2:mem:")) {
            try {
              val conn = DriverManager.getConnection(
                url,
                Try(c.getString("user")).getOrElse(""),
                Try(c.getString("password")).getOrElse(""),
              )
              try conn.createStatement().execute("SHUTDOWN")
              finally conn.close()
            } catch {
              case NonFatal(e) => noTracingLogger.info(s"Could not drop H2 database $url: $e")
            }
          }
        case _ => ()
      }
}
