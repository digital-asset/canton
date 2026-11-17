// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.SequencerSubscriptionPoolTest.{
  handlerException,
  someEvent,
  subscriptionException,
  validationError,
}
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.PreviousTimestampMismatch
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.EventValidationError
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.{
  TrustAssumptionViolated,
  UnreachableTrustThreshold,
}
import com.digitalasset.canton.sequencing.client.pool.{
  ConnectionPoolTestHelpers,
  SequencerSubscriptionWrapper,
  SequencerSubscriptionWrapperImpl,
}
import com.digitalasset.canton.sequencing.client.{
  SequencerClientSubscriptionError,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

class SequencerSubscriptionPoolTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  "SequencerSubscriptionPool" should {
    "initialize in the happy path" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(2)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()

        clue("normal start") {
          listener.shouldStabilizeOnState(ComponentHealthState.Ok())
          subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
        }
        val initialPool = subscriptionPool.subscriptions.toSeq

        clue("Fail connections") {
          initialPool.foreach(_.connection.fail(reason = "test"))

          // Connections eventually come back
          eventually() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
          }
        }
      }
    }

    "handle configuration changes" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(3)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()

        listener.shouldStabilizeOnState(ComponentHealthState.Ok())
        subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative

        clue("Increase liveness margin") {
          val newLivenessMargin = NonNegativeInt.tryCreate(4)
          val newConfig =
            subscriptionPool.config.copy(livenessMargin = newLivenessMargin)
          subscriptionPool.updateConfig(newConfig)

          eventually() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + newLivenessMargin).toNonNegative
          }
        }

        clue("Decrease liveness margin") {
          val newLivenessMargin = NonNegativeInt.tryCreate(1)
          val newConfig =
            subscriptionPool.config.copy(livenessMargin = newLivenessMargin)
          subscriptionPool.updateConfig(newConfig)

          // eventuallyForever() to check we don't remove more than necessary
          eventuallyForever() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + newLivenessMargin).toNonNegative
          }
        }
      }
    }

    "request connections as needed" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(2)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()

        clue("normal start") {
          listener.shouldStabilizeOnState(ComponentHealthState.Ok())
          subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
        }
        val initialPool = subscriptionPool.subscriptions.toSeq
        val initialConnections = initialPool.map(_.connection.name).toSet

        clue("Gradually remove 3 active connections") {
          initialPool.take(3).zipWithIndex.foreach { case (subscription, index) =>
            subscription.connection.fatal(reason = "test")
            eventually() {
              subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
              // New connections have been obtained
              val currentConnections = subscriptionPool.subscriptions.map(_.connection.name)
              val delta = initialConnections.diff(currentConnections)
              // The newly obtained connections are different
              delta should have size (index.toLong + 1)
            }
          }
        }

        clue("Remove 1 more to go below the active threshold") {
          initialPool(3).connection.fatal(reason = "test")

          listener.shouldStabilizeOnState(
            ComponentHealthState.degraded(
              "below liveness margin: 6 subscription(s) available, trust threshold = 5, liveness margin = 2"
            )
          )
          subscriptionPool.nbSubscriptions shouldBe NonNegativeInt.tryCreate(6)
        }

        clue("Remove 2 more to go below the trust threshold") {
          initialPool.slice(4, 6).foreach(_.connection.fatal(reason = "test"))

          // Subscription pool gets closed because the threshold is no longer reachable
          listener.shouldStabilizeOnAssertion {
            case fatal: ComponentHealthState.Fatal =>
              fatal.state.description.value should include(
                "Trust threshold of 5 is no longer reachable"
              )
            case other => fail(s"bad state: $other")
          }
          // It does not make sense to check for the number of remaining subscriptions in the pool at this point
          // since the pool has been closed.
        }
      }
    }

    "keep retrying and report a failed health when trust threshold sequencers are faulty" in {
      withConnectionAndSubscriptionPools(
        nbConnections = PositiveInt.tryCreate(4),
        trustThreshold = PositiveInt.three,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = NonNegativeInt.zero,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()
        listener.shouldStabilizeOnState(ComponentHealthState.Ok())

        val initialSubscriptions = subscriptionPool.subscriptions.toSeq
        initialSubscriptions should have size 3

        clue("a single sequencer failing the validation is replaced") {
          loggerFactory.assertLogs(
            closeSubscription(initialSubscriptions(0), validationError),
            _.warningMessage should include(
              "Closing sequencer subscription due to event validation error"
            ),
          )

          eventually(maxPollInterval = 100.millis) {
            val subscriptions = subscriptionPool.subscriptions
            subscriptions should have size 3
            subscriptions should not contain initialSubscriptions(0)
          }
          subscriptionPool.health.isOk shouldBe true
        }

        clue(
          "a second sequencer crashing the handler reports threshold not reachable and makes the pool failed"
        ) {
          loggerFactory.assertLogs(
            closeSubscription(initialSubscriptions(1), handlerException),
            _.warningMessage should include(
              "Closing sequencer subscription due to handler exception"
            ),
            _.shouldBeCantonError(
              UnreachableTrustThreshold.code,
              _ should include("2 remaining sequencer(s), 2 faulty"),
            ),
          )

          eventually(maxPollInterval = 100.millis) {
            val subscriptions = subscriptionPool.subscriptions
            subscriptions should have size 3
            subscriptions should not contain initialSubscriptions(1)
          }

          subscriptionPool.health.getState shouldBe a[ComponentHealthState.Failed]
        }

        clue(
          "a third sequencer failing with an exception reports trust assumption violation in addition to new threshold not reachable"
        ) {
          loggerFactory.assertLogs(
            failSubscription(initialSubscriptions(2), subscriptionException),
            logEntry => {
              logEntry.warningMessage should include(
                "Closing sequencer subscription due to exception"
              )
              logEntry.throwable shouldBe Some(subscriptionException)
            },
            _.shouldBeCantonError(
              TrustAssumptionViolated.code,
              _ should include("3 sequencer(s) have caused failures"),
            ),
            _.shouldBeCantonError(
              UnreachableTrustThreshold.code,
              _ should include("1 remaining sequencer(s), 3 faulty"),
            ),
          )

          eventually(maxPollInterval = 100.millis) {
            val subscriptions = subscriptionPool.subscriptions
            subscriptions should have size 3
            subscriptions should not contain initialSubscriptions(2)
          }
          subscriptionPool.health.getState shouldBe a[ComponentHealthState.Failed]
          subscriptionPool.completion.isCompleted shouldBe false
        }

        clue("further failures are only warnings") {
          loggerFactory.assertLogsUnorderedOptional(
            closeSubscription(subscriptionPool.subscriptions.head, handlerException),
            LogEntryOptionality.Required -> (_.warningMessage should include(
              "Closing sequencer subscription due to handler exception"
            )),
            // Optional because it depends on whether the new failed subscription is on a previously failed sequencer
            LogEntryOptionality.Optional -> (_.shouldBeCantonErrorCode(
              UnreachableTrustThreshold.code
            )),
          )
          eventually(maxPollInterval = 100.millis) {
            subscriptionPool.subscriptions should have size 3
          }
          subscriptionPool.health.getState shouldBe a[ComponentHealthState.Failed]
        }

        clue("the pool recovers when an event is aggregated") {
          subscriptionPool.checkLiveness(someEvent)
          listener.shouldStabilizeOnState(ComponentHealthState.Ok())
          subscriptionPool.completion.isCompleted shouldBe false
        }
      }
    }

    "not report a failed health when faulty sequencers do not prevent progress" in {
      withConnectionAndSubscriptionPools(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.two,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = NonNegativeInt.zero,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()
        listener.shouldStabilizeOnState(ComponentHealthState.Ok())

        val initialSubscriptions = subscriptionPool.subscriptions.toSeq
        initialSubscriptions should have size 2

        // Only warnings: an error would fail the assertion
        loggerFactory.assertLogs(
          {
            closeSubscription(initialSubscriptions(0), validationError)
            eventually(maxPollInterval = 100.millis) {
              subscriptionPool.subscriptions should not contain initialSubscriptions(0)
            }

            // An event is aggregated from the remaining subscriptions
            subscriptionPool.checkLiveness(someEvent)

            closeSubscription(initialSubscriptions(1), handlerException)
            eventually(maxPollInterval = 100.millis) {
              val subscriptions = subscriptionPool.subscriptions
              subscriptions should have size 2
              subscriptions should not contain initialSubscriptions(1)
            }
          },
          _.warningMessage should include("due to event validation error"),
          _.warningMessage should include("due to handler exception"),
        )
        listener.shouldStabilizeOnState(ComponentHealthState.Ok())
      }
    }

    "keep retrying the same sequencer when it is the only one and its handler crashes" in {
      withConnectionAndSubscriptionPools(
        nbConnections = PositiveInt.one,
        trustThreshold = PositiveInt.one,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = NonNegativeInt.zero,
      ) { (subscriptionPool, listener) =>
        subscriptionPool.start()
        listener.shouldStabilizeOnState(ComponentHealthState.Ok())

        val initialSubscription = subscriptionPool.subscriptions.loneElement

        loggerFactory.assertLogs(
          closeSubscription(initialSubscription, handlerException),
          _.warningMessage should include(
            "Closing sequencer subscription due to handler exception: java.lang.RuntimeException: test handler crash"
          ),
          _.shouldBeCantonError(TrustAssumptionViolated.code),
          _.shouldBeCantonError(UnreachableTrustThreshold.code),
        )

        def sequencerIdOf(subscription: SequencerSubscriptionWrapper): SequencerId =
          subscription.connection.attributes.sequencerId

        // The same sequencer is subscribed to again
        eventually(maxPollInterval = 100.millis) {
          val subscription = subscriptionPool.subscriptions.loneElement
          subscription should not be initialSubscription
          sequencerIdOf(subscription) should be(sequencerIdOf(initialSubscription))
        }
        subscriptionPool.health.getState shouldBe a[ComponentHealthState.Failed]
        subscriptionPool.completion.isCompleted shouldBe false
      }
    }
  }

  private def closeSubscription(
      subscription: SequencerSubscriptionWrapper,
      reason: SubscriptionCloseReason[SequencerClientSubscriptionError],
  ): Unit = subscription match {
    case impl: SequencerSubscriptionWrapperImpl => impl.completeSubscription(reason)
    case other => fail(s"Unexpected subscription wrapper: $other")
  }

  private def failSubscription(
      subscription: SequencerSubscriptionWrapper,
      exception: Throwable,
  ): Unit = subscription match {
    case impl: SequencerSubscriptionWrapperImpl => impl.failSubscription(exception)
    case other => fail(s"Unexpected subscription wrapper: $other")
  }
}

object SequencerSubscriptionPoolTest {

  private val subscriptionException = new RuntimeException("test subscription failure")

  private val handlerException: SubscriptionCloseReason[SequencerClientSubscriptionError] =
    SubscriptionCloseReason.HandlerException(new RuntimeException("test handler crash"))

  private def someEvent(implicit traceContext: TraceContext): EventAndOrdinal =
    EventAndOrdinal.zero(
      IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(0), None, None)(traceContext)
    )

  private val validationError: SubscriptionCloseReason[SequencerClientSubscriptionError] =
    SubscriptionCloseReason.HandlerError(
      EventValidationError(
        PreviousTimestampMismatch(
          receivedPreviousTimestamp = None,
          expectedPreviousTimestamp = Some(CantonTimestamp.Epoch),
        )
      )
    )
}
