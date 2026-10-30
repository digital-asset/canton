// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.data.EitherT
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.ParticipantServices
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.replica.ParticipantReplicaManager
import com.digitalasset.canton.participant.store.ParticipantNodePersistentState
import com.digitalasset.canton.participant.store.memory.MutablePackageMetadataViewImpl
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.ParticipantTopologyDispatcher
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackendImpl
import com.digitalasset.canton.scheduler.Schedulers
import com.digitalasset.canton.tea.TrafficEnforcementApp
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.mockito.Mockito.{CALLS_REAL_METHODS, inOrder as mockOrder}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ParticipantTrafficEnforcementLifecycleTest extends BaseTestWordSpec with HasExecutionContext {
  private class Manager
      extends ParticipantReplicaManager(false, timeouts, loggerFactory, futureSupervisor) {
    def activate(): FutureUnlessShutdown[Unit] = transitionToActive()
  }

  private class Fixture(enabled: Boolean = true) extends AutoCloseable {
    val app = mock[LifeCycleContainer[TrafficEnforcementApp]]
    val backend = mock[LifeCycleContainer[TrafficEnforcementBackendImpl]]
    val services = ParticipantServices(
      persistentStateContainer = mock[LifeCycleContainer[ParticipantNodePersistentState]],
      mutablePackageMetadataView = mock[MutablePackageMetadataViewImpl],
      ledgerApiIndexerContainer = mock[LifeCycleContainer[LedgerApiIndexer]],
      trafficEnforcementBackendContainerO = Option.when(enabled)(backend),
      trafficEnforcementAppContainerO = Option.when(enabled)(app),
      cantonSyncService = mock[CantonSyncService],
      ledgerApiIndexServiceContainer = mock[LifeCycleContainer[LedgerApiIndexService]],
      schedulers = mock[Schedulers],
      acsReplicatorContainerO = None,
      partyReplicatorContainerO = None,
      ledgerApiServerContainer = mock[LifeCycleContainer[LedgerApiServer]],
      startableStoppableLedgerApiDependentServices =
        mock[StartableStoppableLedgerApiDependentServices],
      participantTopologyDispatcher = mock[ParticipantTopologyDispatcher],
      acsCommitmentProcessorManagerO = None,
    )
    Seq[LifeCycleContainer[?]](
      app,
      backend,
      services.persistentStateContainer,
      services.ledgerApiIndexerContainer,
      services.ledgerApiIndexServiceContainer,
      services.ledgerApiServerContainer,
    ).foreach { container =>
      when(container.initializeNext()).thenReturn(FutureUnlessShutdown.unit)
      when(container.currentAutoCloseable()).thenCallRealMethod()
      ()
    }
    when(services.mutablePackageMetadataView.refreshState(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)
    when(services.cantonSyncService.refreshCaches()(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)
    when(services.cantonSyncService.finishLSUs()(anyTraceContext)).thenReturn(EitherT.rightT(()))
    when(
      services.cantonSyncService.reconnectSynchronizers(eqTo(true), eqTo(false), eqTo(false))(
        anyTraceContext
      )
    )
      .thenReturn(EitherT.rightT(Seq.empty))
    when(services.schedulers.start()(anyTraceContext)).thenReturn(FutureUnlessShutdown.unit)
    val manager = new Manager
    manager.setInitialized(services)
    override def close(): Unit = manager.close()
  }

  "TEA lifecycle" should {
    Seq[(String, Fixture => FutureUnlessShutdown[Unit])](
      "backend" -> (_.backend.initializeNext()),
      "Ledger API" -> (_.services.ledgerApiServerContainer.initializeNext()),
      "schedulers" -> (_.services.schedulers.start()),
    ).foreach { case (name, initialize) =>
      s"close TEA when $name initialization fails or shuts down" in {
        val error = new IllegalStateException("initialization failed")
        forAll(
          Table[Try[UnlessShutdown[Unit]]](
            "result",
            Failure(error),
            Success(UnlessShutdown.AbortedDueToShutdown),
          )
        ) { result =>
          withResource(new Fixture) { f =>
            when(initialize(f)).thenReturn(FutureUnlessShutdown(Future.fromTry(result)))
            f.manager.activate().unwrap.transform(Success(_)).futureValue shouldBe result
            val order = mockOrder(f.backend, f.app)
            order.verify(f.backend).closeCurrent()
            order.verify(f.app).closeCurrent()
            verify(f.services.ledgerApiServerContainer, never).closeCurrent()
          }
        }
      }
    }

    "start TEA before the Ledger API and keep it running on success, or leave it disabled" in {
      forAll(Table("enabled", true, false)) { enabled =>
        withResource(new Fixture(enabled)) { f =>
          f.manager.activate().futureValueUS
          val order = mockOrder(f.app, f.backend, f.services.ledgerApiServerContainer)
          if (enabled) {
            order.verify(f.app).initializeNext().discard
            order.verify(f.backend).initializeNext().discard
          } else {
            verify(f.app, never).initializeNext().discard
            verify(f.backend, never).initializeNext().discard
          }
          order.verify(f.services.ledgerApiServerContainer).initializeNext().discard
          verify(f.backend, never).closeCurrent()
          verify(f.app, never).closeCurrent()
        }
      }
    }

    "close the backend before the app on node close" in {
      withResource(new Fixture) { f =>
        val node = mock[ParticipantNode](CALLS_REAL_METHODS)
        when(node.participantServices).thenReturn(f.services)
        when(node.loggerFactory).thenReturn(loggerFactory)
        node.close()
        val order = mockOrder(f.backend, f.app)
        order.verify(f.backend).closeCurrent()
        order.verify(f.app).closeCurrent()
        verify(f.services.ledgerApiServerContainer, never).closeCurrent()
      }
    }
  }
}
