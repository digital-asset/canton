// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.LocalMediatorReference
import com.digitalasset.canton.crypto.{SyncCryptoApi, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.IntegrationTestUtilities.*
import com.digitalasset.canton.integration.{CommunityIntegrationTest, TestConsoleEnvironment}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.messages.{ConfirmationResultMessage, SignedProtocolMessage}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.synchronizer.mediator.MediatorVerdict.MediatorReject
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Minutes, Seconds, Span}
import org.slf4j.event.Level

import scala.concurrent.Future
import scala.concurrent.duration.*

trait SequencerRestartTest extends InFlightAggregationTestHelper { self: CommunityIntegrationTest =>

  // Those test cases may interfere with each other by sending submissions in the background.
  // Some of the submissions, which started as part of one test case, may time out as part of the next one.
  private val optionalManySubmissionTimedOut: LogEntry => Assertion =
    _.warningMessage should include("Submission timed out at")

  // Due to sequencer restart time proof requests get retried, and may get logged as warning after reaching a threshold.
  private val optionalManyRequestCurrentTime: LogEntry => Assertion =
    _.warningMessage should include("Now retrying operation 'request current time'")

  protected def name: String

  protected def restartSequencers()(implicit env: TestConsoleEnvironment): Unit

  s"environment using a restartable $name sequencer" should {
    val timeout = NonNegativeDuration.tryFromDuration(4.minutes)
    "be able to restart sequencer between different activities" in { implicit env =>
      import env.*

      val time = participant1.health.ping(participant2, timeout = timeout)
      logger.info(s"P1 pings p2 in $time. Restarting.")

      loggerFactory.suppressWarningsAndErrors {
        restartSequencers()

        logger.info(s"Restarted. Performing a second ping.")
        val time2 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time2.")
      }
    }

    "be able to restart sequencer during activity" in { implicit env =>
      import env.*
      val time = participant1.health.ping(env.participant2, timeout = timeout)
      logger.info(s"P1 pings p2 in $time.")

      val p1Count = grabCounts(daName, participant1, limit = 250)

      val levels: Int = 6

      loggerFactory.suppressWarningsAndErrors {
        val restartF = Future {
          val before = poll(40.seconds, 10.milliseconds) {
            val p1Count2 = grabCounts(daName, participant1, limit = 250)
            // wait until a quarter of the levels have been dealt with
            assert(
              p1Count2.acceptedTransactionCount - p1Count.acceptedTransactionCount >= math
                .pow(2, (levels + 1d) / 4),
              s"A quarter of levels have not yet been dealt with",
            )
            // make sure the bong hasn't finished
            assert(
              p1Count2.acceptedTransactionCount - p1Count.acceptedTransactionCount <= 3 * math
                .pow(2, (levels + 1d) / 4),
              s"Bong almost finished",
            )
            p1Count2.pcsCount
          }

          logger.info(s"Restarting sequencers at pcs count $before")
          restartSequencers()
          logger.info("Successfully restarted sequencers")

          val after = grabCounts(daName, participant1, limit = 250).pcsCount
          logger.info(s"After restart, $participant1 has pcs count $after")
        }

        val bongF = Future {
          import env.*
          participant2.testing.bong(
            targets = Set(participant1.id, participant2.id),
            validators = Set(participant1.id),
            levels = levels,
            timeout = 5.minutes,
          )
        }

        val patience = defaultPatience.copy(timeout = 3.minutes)
        restartF.futureValue(patience, Position.here)

        logger.info(s"Performing a second ping, after restart")
        val time2 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time2, after restart")

        val bongDuration = bongF.futureValue(Timeout(Span(5, Minutes)))
        logger.info(s"Bong completed after $bongDuration")

        // The bong above might lead to some ongoing activity -- if the test suite finishes and
        // Canton is torn down, this might lead to the slower bong responses to still be ongoing
        // but not being able to complete cleanly. This is fine but running a ping here between
        // the participants involved in the previous bong ensures that we block here until all
        // transactions are processed and we don't cause unwanted noise during shutdown. This
        // works under the assumption that at this point all the bong responses are being
        // processed by the sequencer.
        logger.info(s"Performing a third ping, after bong")
        val time3 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time3, after bong")
      }
    }

    "restart while aggregating submissions" in { implicit env =>
      import env.*

      // Ensure the test meets our requirements (two mediators with threshold 2)
      val med =
        participant1.topology.mediators
          .list(sequencers.all.headOption.value.physical_synchronizer_id)
          .loneElement
          .item
      assertResult(2)(med.active.size)
      assertResult(2)(med.threshold.value)

      val ts = env.environment.clock.now
      val maxTs = ts.plusSeconds(300)
      val timestamps = SendRequestTimestamps(
        topologyTimestamp = None,
        approximateTimestampForSigning = ts,
        maxSequencingTime = maxTs,
      )

      def sendAndWait(mediator: LocalMediatorReference): Unit = {

        val (client, cryptoApi) = sequencerClientAndCryptoApiOf(mediator)
        val batch = this.createAggregationResultMessage(
          cryptoApi,
          1,
          ts,
        )
        val callback = SendCallback.future
        val sendAsync = client.send(
          batch,
          timestamps = timestamps,
          aggregationRule =
            Some(AggregationRule.activeMediators(MediatorGroupIndex.zero, testedProtocolVersion)),
          callback = callback,
        )(traceContext, MetricsContext.Empty)
        sendAsync.valueOrFailShutdown("Mediator send").futureValue
        callback.future
          .onShutdown(fail("shutdown"))
          .futureValue(Timeout(Span(45, Seconds))) should matchPattern {
          case SendResult.Success(_) =>
        }
      }

      clue("Sending first part of the aggregatable submission from mediator 1") {
        sendAndWait(mediator1)
      }

      loggerFactory.suppressWarningsAndErrors {
        clue("Restart sequencers") {
          restartSequencers()
          // Send a ping to make sure that both participants are connected again
          participant1.health.ping(participant2, timeout = timeout)
        }
      }

      loggerFactory.assertEventuallyLogsSeq(
        (SuppressionRule.LevelAndAbove(Level.INFO) && SuppressionRule.LoggerNameContains(
          "TransactionProcessor"
        )) ||
          SuppressionRule.LevelAndAbove(Level.WARN)
      )(
        sendAndWait(mediator2),
        logs => {
          val assertInvalidRequest: LogEntry => Assertion =
            _.message should include("for request that is not pending")
          if (logs.sizeIs > 1) {
            forAtMost(logs.size - 1, logs)(optionalManySubmissionTimedOut)
            forAtMost(logs.size - 1, logs)(optionalManyRequestCurrentTime)
          }

          forAtLeast(1, logs)(assertInvalidRequest)
        },
      )
    }
  }
}

trait InFlightAggregationTestHelper {
  self: CommunityIntegrationTest =>

  protected def createAggregationResultMessage(
      cryptoOp: SyncCryptoApi,
      digestSeed: Int,
      requestId: CantonTimestamp,
  )(implicit
      env: TestConsoleEnvironment
  ): Batch[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] = {
    import env.*

    // note, we simulate here the rejection of a bogus request.
    // the purpose of the test though is just to check that aggregation works across sequencer
    // switches. but with pv35 we only allow aggregations for sequencers and mediators, which
    // limits the types of messages we can send.
    val resultMessage = ConfirmationResultMessage.create(
      env.sequencers.all.headOption.value.physical_synchronizer_id,
      TransactionViewType,
      RequestId(requestId),
      RootHash(TestHash.digest(digestSeed)),
      MediatorReject(reason = MediatorError.MalformedMessage.Reject(s"Test$digestSeed"))
        .toVerdict(testedProtocolVersion),
    )
    Batch.of(
      testedProtocolVersion,
      SignedProtocolMessage.signAndCreate(resultMessage, cryptoOp, None).futureValueUS.value
        -> Recipients.cc(env.participant1.id),
    )
  }

  def sequencerClientAndCryptoApiOf(
      mediator: LocalMediatorReference
  ): (SequencerClient, SyncCryptoApi) = {
    val tmp = mediator.underlying.value.replicaManager.mediatorRuntime.value.mediator
    (tmp.sequencerClient, tmp.syncCrypto.currentSnapshotApproximation.futureValueUS)
  }

}
