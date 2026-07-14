// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.console.{LocalParticipantReference, LocalSequencerReference}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInconsistentConnectivity
import com.digitalasset.canton.resource.{DbStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.concurrent.Future

/** This test covers:
  *   - the ability of participant with/without dev version to connect to synchronizer with/without
  *     dev version
  *   - if the dev migrations have been run
  */
sealed trait DevVersionSupportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with FlagCloseable
    with HasCloseContext {

  protected val sequencerGroups: MultiSynchronizer =
    DevOrAlphaSupportTestEnvironment.sequencerGroups

  override lazy val environmentDefinition: EnvironmentDefinition =
    DevOrAlphaSupportTestEnvironment.createEnvironment(
      testedProtocolVersion = testedProtocolVersion,
      versionSupport = DevOrAlphaSupportTestEnvironment.DevVersionSupport,
      synchronizer1ProtocolVersion = ProtocolVersion.dev,
    )

  private def checkIfSequencerDevMigrationsHavBeenRun(
      node: LocalSequencerReference
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    checkIfDevMigrationsHaveBeenRun(
      node.underlying.getOrElse(fail(s"Sequencer node $node not found")).sequencer.storage
    )

  private def checkIfParticipantDevMigrationsHaveBeenRun(
      node: LocalParticipantReference
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    checkIfDevMigrationsHaveBeenRun(
      node.underlying.getOrElse(fail(s"Participant node $node not found")).storage
    )

  private def checkIfDevMigrationsHaveBeenRun(
      storage: Storage
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Boolean] = {
    val result = storage match {
      case dbStorage: DbStorage =>
        dbStorage.query(sql"SELECT test_column FROM dev_migration_test".as[Int], "").map(_ => true)
      case _ =>
        FutureUnlessShutdown.failed(
          new NotImplementedError("dev migration check does not make sense on the in-memory store")
        )
    }

    result.failOnShutdown
  }

  private def expectedLogErrors(message: String): Assertion =
    message should (
      include("does not exist") // postgres
        or
          include("not found") // H2
    )

  "synchronizer1" should {
    "have extra migration" in { env =>
      checkIfSequencerDevMigrationsHavBeenRun(env.sequencer1).futureValue shouldBe true
    }
  }

  "synchronizer2" should {
    "have extra migration when all nodes use dev version" onlyRunWhen (_.isDev) in { env =>
      checkIfSequencerDevMigrationsHavBeenRun(env.sequencer2).futureValue shouldBe true
    }

    "have no extra migration when not all nodes use dev version" onlyRunWhen (!_.isDev) in { env =>
      expectedLogErrors(
        checkIfSequencerDevMigrationsHavBeenRun(env.sequencer2).failed.futureValue.getMessage
      )
    }
  }

  // If testedProtocolVersion is dev, then the participants are also using an dev protocol version. In that case, this test is wrong.
  "participant without dev version" should {
    "have no extra migration" onlyRunWhen (!_.isDev) in { implicit env =>
      expectedLogErrors(
        checkIfParticipantDevMigrationsHaveBeenRun(env.participant2).failed.futureValue.getMessage
      )
    }

    "not be able to connect to a synchronizer with dev" onlyRunWhen (!_.isDev) in { env =>
      assertThrowsAndLogsCommandFailures(
        env.participant2.synchronizers.connect(env.sequencer1, env.daName),
        entry => {
          entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
          entry.message should include(
            s"The protocol version required by the server (${ProtocolVersion.dev}) is not among the supported protocol versions by the client"
          )
        },
      )
    }

    "succeed subsequently on synchronizer without dev version" onlyRunWhen (!_.isDev) in { env =>
      env.participant2.synchronizers
        .connect_local(env.sequencer2, alias = env.acmeName)
    }
  }

  "participant with dev version" should {
    "have extra migration" in { env =>
      checkIfParticipantDevMigrationsHaveBeenRun(env.participant1).futureValue shouldBe true
    }

    "connect to a synchronizer with dev" in { env =>
      env.participant1.synchronizers
        .connect_local(env.sequencer1, alias = env.daName)
      env.participant1.health.ping(env.participant1.id)
    }

    "also succeed on synchronizer without dev" in { env =>
      import env.*

      participant1.synchronizers.connect(sequencer2, acmeName)
      participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant2.health.ping(participant1.id)
    }
  }
}

class DevVersionSupportIntegrationTestH2 extends DevVersionSupportIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}

class DevVersionSupportIntegrationTestPostgres extends DevVersionSupportIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}
