// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.platform.apiserver.services.metrics.{
  TrafficEnforcementInventory,
  TrafficEnforcementMetrics,
}
import com.digitalasset.canton.tea.TrafficEnforcementErrors.{
  CorrelationConflict,
  InsufficientBalance,
  UnsafeToPrune,
}
import com.digitalasset.canton.tea.projection.memory.TeaMemoryTrafficStore
import com.digitalasset.canton.tea.projection.{
  AccountId,
  CorrelationAccount,
  CorrelationKey,
  EventId,
  EventSource,
  SubmissionCorrelation,
  TrafficDelta,
}
import com.digitalasset.canton.tea.v1.RegisterSubmissionRequest.ChargedAccount
import com.digitalasset.canton.tea.v1.{
  PruneRequest,
  PruneResponse,
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

class TrafficEnforcementServiceTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  private def newService(
      store: TeaMemoryTrafficStore,
      clock: SimClock,
      retention: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7),
  ): TrafficEnforcementService =
    new TrafficEnforcementService(
      store,
      clock,
      retention,
      new TrafficEnforcementMetrics(
        inventory = new TrafficEnforcementInventory(MetricName("test"))(new HistogramInventory()),
        metricsFactory = new InMemoryMetricsFactory,
      )(MetricsContext.Empty),
      loggerFactory,
    )

  "prune" should {
    "only prune data older than the minimum retention" in {
      val retention = NonNegativeFiniteDuration.ofDays(7)
      val alice = AccountId.tryCreate("alice")
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = newService(store, clock, retention)
      val seededAt = clock.now
      val request = PruneRequest(Some(seededAt.toProtoTimestamp))

      for {
        _ <- store
          .persistTrafficDelta(
            alice,
            EventId.tryCreate("event"),
            EventSource.TeaAPI,
            TrafficDelta.creditBalanceDelta(10),
            seededAt,
          )
          .valueOrFail("persist")
        _ <- store
          .insertCorrelation(
            SubmissionCorrelation(
              CorrelationKey(DefaultDamlValues.lfhash(), "submission"),
              CorrelationAccount.Account(alice),
              seededAt,
            )
          )
          .valueOrFail("insert correlation")
        // Try pruning before the retention period has passed. This should be rejected.
        rejected <- service.prune(request)
        // Move the clock outside the retention boundary is so pruning is allowed.
        _ = clock.advance(retention.asJava)
        pruned <- service.prune(request)
      } yield {
        inside(rejected) { case Left(error) =>
          error.code.id shouldBe UnsafeToPrune.id
          error.context should contain(
            "safe_before_or_at" -> seededAt.minus(retention.asJava).toString
          )
        }
        pruned shouldBe Right(PruneResponse(prunedEventCount = 1, prunedCorrelationCount = 1))
      }
    }
  }

  "registerSubmission" should {
    "check the balance before storing the correlation" in {
      val alice = AccountId.tryCreate("alice")
      val bob = AccountId.tryCreate("bob")
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = newService(store, clock)
      val registeredAt = clock.now.plusSeconds(1)
      val aliceKey = CorrelationKey(DefaultDamlValues.lfhash(1), "alice-submission")
      val bobKey = CorrelationKey(DefaultDamlValues.lfhash(2), "bob-submission")
      val markerKey = CorrelationKey(DefaultDamlValues.lfhash(3), "marker-submission")

      def request(
          key: CorrelationKey,
          accountId: Option[String],
          requiredBalance: Option[Long],
      ) = RegisterSubmissionRequest(
        changeIdHash = key.changeIdHash.toHexString,
        submissionId = key.submissionId,
        account = accountId.map(ChargedAccount(_, requiredBalance)),
      )

      for {
        _ <- store
          .persistTrafficDelta(
            alice,
            EventId.tryCreate("event"),
            EventSource.TeaAPI,
            TrafficDelta.creditBalanceDelta(10),
            clock.now,
          )
          .valueOrFail("persist")
        _ = clock.advanceTo(registeredAt)
        tooLow <- service.registerSubmission(request(aliceKey, Some("alice"), Some(11L)))
        afterTooLow <- store.lookupCorrelation(aliceKey).valueOrFail("lookup after too low")
        enough <- service.registerSubmission(request(aliceKey, Some("alice"), Some(10L)))
        afterEnough <- store.lookupCorrelation(aliceKey).valueOrFail("lookup after enough")
        repeated <- service.registerSubmission(request(aliceKey, Some("alice"), None))
        unknownChecked <- service.registerSubmission(request(bobKey, Some("bob"), Some(1L)))
        afterUnknownChecked <- store.lookupCorrelation(bobKey).valueOrFail("lookup bob checked")
        unchecked <- service.registerSubmission(request(bobKey, Some("bob"), None))
        afterUnchecked <- store.lookupCorrelation(bobKey).valueOrFail("lookup bob")
        marker <- service.registerSubmission(request(markerKey, None, None))
        afterMarker <- store.lookupCorrelation(markerKey).valueOrFail("lookup marker")
      } yield {
        inside(tooLow) { case Left(error) => error.code.id shouldBe InsufficientBalance.id }
        afterTooLow shouldBe None
        enough shouldBe Right(RegisterSubmissionResponse())
        afterEnough shouldBe Some(
          SubmissionCorrelation(aliceKey, CorrelationAccount.Account(alice), registeredAt)
        )
        inside(repeated) { case Left(error) => error.code.id shouldBe CorrelationConflict.id }
        inside(unknownChecked) { case Left(error) =>
          error.code.id shouldBe InsufficientBalance.id
        }
        afterUnknownChecked shouldBe None
        unchecked shouldBe Right(RegisterSubmissionResponse())
        afterUnchecked shouldBe Some(
          SubmissionCorrelation(bobKey, CorrelationAccount.Account(bob), registeredAt)
        )
        marker shouldBe Right(RegisterSubmissionResponse())
        afterMarker shouldBe Some(
          SubmissionCorrelation(markerKey, CorrelationAccount.NoAccountSelected, registeredAt)
        )
      }
    }

    "reject malformed requests without storing anything" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = newService(store, clock)
      val key = CorrelationKey(DefaultDamlValues.lfhash(), "submission")

      for {
        badHash <- service.registerSubmission(
          RegisterSubmissionRequest("not-hex", "submission", Some(ChargedAccount("alice", None)))
        )
        noSubmissionId <- service.registerSubmission(
          RegisterSubmissionRequest(
            key.changeIdHash.toHexString,
            "",
            Some(ChargedAccount("alice", None)),
          )
        )
        emptyAccount <- service.registerSubmission(
          RegisterSubmissionRequest(
            key.changeIdHash.toHexString,
            key.submissionId,
            Some(ChargedAccount("", None)),
          )
        )
        stored <- store.lookupCorrelation(key).valueOrFail("lookup")
      } yield {
        forEvery(Seq(badHash, noSubmissionId, emptyAccount)) { result =>
          inside(result) { case Left(error) =>
            error.code.id shouldBe ProtoDeserializationFailure.id
          }
        }
        stored shouldBe None
      }
    }
  }
}
