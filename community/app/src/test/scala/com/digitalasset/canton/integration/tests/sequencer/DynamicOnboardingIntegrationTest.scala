// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.metrics.api.MetricsContext
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.admin.api.client.data.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.client.SendResult
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

abstract class DynamicOnboardingIntegrationTest(val name: String)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode
    with ReliabilityTestSuite
    with InFlightAggregationTestHelper {

  protected val synchronizerInitializationTimeout: Duration = Duration.ofSeconds(20)
  implicit private val metricsContext: MetricsContext = MetricsContext.Empty

  /** Hook for allowing sequencer integrations to adjust the base test config */
  protected def additionalConfigTransforms: ConfigTransform = ConfigTransforms.identity

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 3,
      numSequencers = 2,
      numMediators = 2,
    ).withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(EnvironmentDefinition.S1M2)
    }.addConfigTransforms(
      ConfigTransforms.setExitOnFatalFailures(false),
      _.focus(_.parameters.timeouts.processing.sequencerInfo)
        .replace(NonNegativeDuration.ofSeconds(5)),
    )

  private def modifyConnection(
      participant: ParticipantReference,
      name: SynchronizerAlias,
      connection: SequencerConnection,
  ): Unit = {
    participant.synchronizers.disconnect(name)
    participant.synchronizers.modify(
      name,
      _.copy(sequencerConnections = SequencerConnections.single(connection)),
    )
    participant.synchronizers.reconnect(name)
  }

  private var aggregationSequenced2: CantonTimestamp = _
  private var requestId: CantonTimestamp = _
  private var maxSequencingTimeOfAggregation: CantonTimestamp = _

  private val aggregationRule: AggregationRule =
    AggregationRule.activeMediators(MediatorGroupIndex.zero, testedProtocolVersion)

  s"using an environment with 2 $name sequencer nodes and a synchronizer configured with only one of them" should {
    "bootstrap a synchronizer and have ping working" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.health.ping(participant3, timeout = 30.seconds)
    }

    "setup in-flight aggregation" in { implicit env =>
      import env.*

      val (m1SequencerClient, m1Crypto) = sequencerClientAndCryptoApiOf(mediator1)

      val now = environment.now
      requestId = now
      maxSequencingTimeOfAggregation = now.add(
        Duration.ofMinutes(2)
      ) // cannot exceed the DynamicSynchronizerParameters.sequencerAggregateSubmissionTimeout (defaults to 5m)

      // First aggregation will remain in-flight while we switch sequencers
      TraceContext.withNewTraceContext("agg1") { implicit traceContext =>
        logger.debug("Sending aggregation 1 part 1")
        val send1ResultPromise = Promise[UnlessShutdown[SendResult]]()

        m1SequencerClient
          .send(
            createAggregationResultMessage(m1Crypto, 1, requestId),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = None,
              approximateTimestampForSigning = now,
              maxSequencingTime = maxSequencingTimeOfAggregation,
            ),
            aggregationRule = Some(aggregationRule),
            callback = send1ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 1 part 1")
          .futureValue
        send1ResultPromise.future.futureValue.onShutdown(fail()).discard[SendResult]
      }

      // Second aggregation is delivered before we switch sequencers, but must be deduplicated afterwards

      val (m2SequencerClient, m2Crypto) = sequencerClientAndCryptoApiOf(mediator2)
      TraceContext.withNewTraceContext("agg2") { implicit traceContext =>
        logger.debug("Sending aggregation 2 part 1")
        val send2ResultPromise = Promise[UnlessShutdown[SendResult]]()
        val send2 = m1SequencerClient
          .send(
            createAggregationResultMessage(m1Crypto, 2, requestId),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = None,
              approximateTimestampForSigning = now,
              maxSequencingTime = maxSequencingTimeOfAggregation,
            ),
            messageId = MessageId.tryCreate("aggregation-2-part-1a"),
            aggregationRule = Some(aggregationRule),
            callback = send2ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 2 part 1a")

        val send3ResultPromise = Promise[UnlessShutdown[SendResult]]()
        val send3 = m2SequencerClient
          .send(
            createAggregationResultMessage(m2Crypto, 2, requestId),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = None,
              approximateTimestampForSigning = now,
              maxSequencingTime = maxSequencingTimeOfAggregation,
            ),
            messageId = MessageId.tryCreate("aggregation-2-part-1b"),
            aggregationRule = Some(aggregationRule),
            callback = send3ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 2 part 1b")

        send2.futureValue
        send3.futureValue
        val send2Result = send2ResultPromise.future.futureValue.onShutdown(fail())
        val send2Timestamp = inside(send2Result) { case SendResult.Success(deliver) =>
          deliver.timestamp
        }
        val send3Result = send3ResultPromise.future.futureValue.onShutdown(fail())
        val send3Timestamp = inside(send3Result) { case SendResult.Success(deliver) =>
          deliver.timestamp
        }
        aggregationSequenced2 = send2Timestamp max send3Timestamp
      }
    }

    "bootstrap command be idempotent and have no effect if called again" in { implicit env =>
      import env.*
      val newSynchronizerId = bootstrap.synchronizer(
        EnvironmentDefinition.S1M2.synchronizerName,
        EnvironmentDefinition.S1M2.sequencers,
        EnvironmentDefinition.S1M2.mediators,
        synchronizerOwners = EnvironmentDefinition.S1M2.synchronizerOwners,
        synchronizerThreshold = EnvironmentDefinition.S1M2.synchronizerThreshold,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        mediatorThreshold = EnvironmentDefinition.S1M2.mediatorThreshold,
      )
      newSynchronizerId shouldBe daId
      participant1.health.ping(participant1, timeout = 30.seconds)
    }

    "participant should be able to ping using dynamically onboarded sequencer" in { implicit env =>
      // TODO(#22198): We should check if the newly onboarded node can serve requests from its onboarding effective time
      //  (in particular up to an end of the containing block in block sequencers) to verify the interface guarantees.
      import env.*
      sequencer1.health.initialized() shouldBe true
      sequencer2.health.initialized() shouldBe false

      onboardNewSequencer(
        synchronizerId = daId,
        newSequencer = sequencer2,
        existingSequencer = sequencer1,
        synchronizerOwners = initializedSynchronizers(daName).synchronizerOwners,
      )

      sequencer2.health.initialized() shouldBe true

      // Restart the new sequencer to make sure that the initialization survives a restart
      // TODO(#25004): restart the BFT sequencer too once it's fully crash fault-tolerant
      // Currently this fails because we're stopping the sequencer before it finished its initial state transfer process.
      // Ideally we wait for that to be concluded before considering the sequencer initialized and ready to tolerate crashes.
      // sequencer2.stop()
      // sequencer2.start()

      // do a self ping to check if seq2 is functional
      participant1.health.ping(participant1)

    }

    "reconnect p1 with threshold=2 and ensure seq2 works" in { implicit env =>
      import env.*
      // we'll connect p2 with threshold=2 so we'll catch ledger forks
      val (config, _, _) = participant1.synchronizers.list_registered().loneElement
      participant1.synchronizers.modify(
        config.synchronizerAlias,
        _.copy(sequencerConnections =
          SequencerConnections.tryMany(
            sequencers.local.map(_.sequencerConnection),
            sequencerTrustThreshold = PositiveInt.two,
            sequencerLivenessMargin = NonNegativeInt.zero,
          )
        ),
      )
      participant1.synchronizers.disconnect_all()
      participant1.synchronizers.reconnect_all()
      val (config2, _, _) = participant1.synchronizers.list_registered().loneElement
      assertResult(2)(config2.sequencerConnections.connections.size)
      participant1.health.ping(participant1)
    }

    "connect new participant2 and ensure newly onboarded sequencer works" in { implicit env =>
      import env.*

      participant2.synchronizers.connect_local(sequencer2, daName)
      participant1.health.ping(participant2, timeout = 30.seconds)
      // restarting the new node after some activity is also an important scenario to check
      sequencer2.stop()
      sequencer2.start()
      participant1.health.ping(participant2, timeout = 30.seconds)
    }

    "participant should be able to switch to new sequencer even if it has received no transactions after the onboarding" taggedAs
      ReliabilityTest(
        Component("Participant", "connected to sequencer"),
        AdverseScenario(
          dependency = "sequencer",
          details = "connected sequencer fails",
        ),
        Remediation(
          remediator = "multiple sequencers",
          action = "participant is manually switched over to use a difference sequencer",
        ),
        outcome = "participant continues to process and submit transactions",
      ) in { implicit env =>
        import env.*

        // This is crucial because the participant will re-request the last event in its sequenced event store
        // and the new sequencer will serve only events from after when the snapshot was taken,
        // which happens before the sequencer onboarding transaction.
        eventually() {
          val sequencerTx = participant3.topology.owner_to_key_mappings.list(
            store = daId,
            filterKeyOwnerUid = sequencer2.id.filterString,
            filterKeyOwnerType = Some(SequencerId.Code),
          )
          // Participant3 has received the onboarding transaction of sequencer2
          sequencerTx should not be empty
        }
        modifyConnection(participant3, daName, sequencer2.sequencerConnection)
        participant3.health.ping(participant1, timeout = 30.seconds)
      }

    "new sequencer correctly aggregates existing in-flight submissions" in { implicit env =>
      import env.*

      // switch over m2 to seq2
      val conn2 = sequencer2.sequencerConnection
      mediator2.sequencer_connection.set(SequencerConnections.single(conn2))
      mediator2.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn2))

      val (m2SequencerClient, m2Crypto) = sequencerClientAndCryptoApiOf(mediator2)
      TraceContext.withNewTraceContext("agg1_2") { implicit traceContext =>
        logger.debug("Sending aggregation 1 part 2")
        val send1ResultPromise = Promise[UnlessShutdown[SendResult]]()
        // This should deliver the bogus reject verdict from the first aggregation
        // or produce a ledger fork if seq2 is inconsistent
        m2SequencerClient
          .send(
            createAggregationResultMessage(m2Crypto, 1, requestId),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = None,
              approximateTimestampForSigning = environment.now,
              maxSequencingTime = maxSequencingTimeOfAggregation,
            ),
            aggregationRule = Some(aggregationRule),
            callback = send1ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 1 part 2")
          .futureValue
        send1ResultPromise.future.futureValue.onShutdown(fail()) shouldBe a[SendResult.Success]
      }

      // Now also try to send the second aggregation that was already delivered
      TraceContext.withNewTraceContext("agg2_2") { implicit traceContext =>
        logger.debug("Sending aggregation 2 part 2")
        val send2ResultPromise = Promise[UnlessShutdown[SendResult]]()
        m2SequencerClient
          .send(
            createAggregationResultMessage(m2Crypto, 2, requestId),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = None,
              approximateTimestampForSigning = environment.now,
              maxSequencingTime = maxSequencingTimeOfAggregation,
            ),
            aggregationRule = Some(aggregationRule),
            callback = send2ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 2 part 2")
          .futureValue
        val send2Result = send2ResultPromise.future.futureValue.onShutdown(fail())
        inside(send2Result) {
          case SendResult.Error(
                DeliverError(
                  _,
                  _,
                  _,
                  _,
                  SequencerErrors.AggregateSubmissionAlreadySent(message),
                  _,
                )
              ) =>
            message should include(s"was previously delivered at $aggregationSequenced2")
          case SendResult.Error(
                DeliverError(
                  _,
                  _,
                  _,
                  _,
                  SequencerErrors.AggregateSubmissionAlreadySentV2(message),
                  _,
                )
              ) =>
            message should include(s"was previously delivered at $aggregationSequenced2")
        }
      }
    }

    "reconnect participant3 to sequencer 1" in { implicit env =>
      logger.debug("reconnect participant3 to sequencer 1")
      import env.*

      modifyConnection(participant3, daName, sequencer1.sequencerConnection)

      participant3.health.ping(participant1, timeout = 30.seconds)
    }

    "participant 3 can eventually change the connection to sequencer 2" in { implicit env =>
      import env.*

      modifyConnection(participant3, daName, sequencer2.sequencerConnection)
      participant3.health.ping(participant1, timeout = 30.seconds)
    }

    "cannot change to an invalid connection" in { implicit env =>
      import env.*

      val invalidConnection =
        GrpcSequencerConnection(
          NonEmpty(Set, Endpoint("fake-host", Port.tryCreate(100))),
          transportSecurity = false,
          None,
          SequencerAlias.Default,
          None,
        )

      val conn = mediator1.sequencer_connection.get()

      loggerFactory.suppressWarningsAndErrors {
        // trying to set the connection to an invalid one will fail the sequencer handshake step
        a[CommandFailure] should be thrownBy mediator1.sequencer_connection.set(
          SequencerConnections.single(invalidConnection)
        )
      }

      // the previous connection details should not have been changed in this case
      mediator1.sequencer_connection.get() shouldBe conn

      // everything still works
      participant1.health.ping(participant2, timeout = 30.seconds)
    }

  }
}
