// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
  CompletionAccountingPolicy,
  CompletionRecord,
  CorrelationAccount,
  CorrelationKey,
  EventId,
  EventSource,
  ProjectionEvent,
  SubmissionCorrelation,
  TrafficDelta,
}
import com.digitalasset.canton.tea.v1.RegisterSubmissionRequest.ChargedAccount
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  ListAccountsPageTokenPayload,
  ListAccountsRequest,
  PruneRequest,
  PruneResponse,
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
  UpdateAccountRequest,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, FailOnShutdown}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.wordspec.AsyncWordSpec

import java.util.Base64

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
        _ <- FutureUnlessShutdown.outcomeF(
          store.applyCompletion(
            ProjectionEvent.Completion(
              1L,
              seededAt,
              CompletionRecord(
                ChangeId(
                  Ref.UserId.assertFromString("user"),
                  Ref.CommandId.assertFromString("uncharged"),
                  Set(Ref.Party.assertFromString("alice")),
                ),
                "uncharged-submission",
                NonNegativeLong.one,
                accepted = true,
              ),
            ),
            "completion",
            EventSource.LedgerAPICompletions,
            CompletionAccountingPolicy(
              debitRejectedCompletions = true,
              adminParty = Ref.Party.assertFromString("admin"),
            ),
          )
        )
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
        pruned shouldBe Right(
          PruneResponse(
            prunedEventCount = 1,
            prunedCorrelationCount = 1,
            prunedUnchargedCompletionCount = 1,
          )
        )
      }
    }
  }

  "listAccounts" should {
    "page through the accounts and filter by prefix" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = newService(store, clock)
      val names = Seq("acme/a", "acme/b", "acme/c", "acmeX", "bob")

      for {
        _ <- names.foldLeft(FutureUnlessShutdown.pure(())) { (previous, name) =>
          previous.flatMap(_ =>
            store
              .persistTrafficDelta(
                AccountId.tryCreate(name),
                EventId.tryCreate(name),
                EventSource.TeaAPI,
                TrafficDelta.creditBalanceDelta(10),
                clock.now,
              )
              .valueOrFail("persist")
              .map(_ => ())
          )
        }
        page1 <- service.listAccounts(listRequest(pageSize = Some(3)))
        page2 <- service.listAccounts(
          listRequest(pageSize = Some(3), pageToken = page1.value.nextPageToken)
        )
        prefixed <- service.listAccounts(listRequest(accountIdPrefix = Some("acme/")))

        emptyPrefixAndToken <- service.listAccounts(
          listRequest(pageToken = Some(""), accountIdPrefix = Some(""))
        )
        exactlyFull <- service.listAccounts(
          listRequest(pageSize = Some(3), accountIdPrefix = Some("acme/"))
        )
        allExactlyFull <- service.listAccounts(listRequest(pageSize = Some(names.size)))
      } yield {
        page1.value.accounts.map(_.accountId) shouldBe Seq("acme/a", "acme/b", "acme/c")
        page1.value.accounts.map(_.balance).distinct shouldBe Seq(10L)
        // Base64 encoding of page token payload
        page1.value.nextPageToken shouldBe Some("CgZhY21lL2M=")
        page2.value.accounts.map(_.accountId) shouldBe Seq("acmeX", "bob")
        page2.value.nextPageToken shouldBe None
        prefixed.value.accounts.map(_.accountId) shouldBe Seq("acme/a", "acme/b", "acme/c")
        prefixed.value.nextPageToken shouldBe None
        // Populated empty prefix acts the same as not unpopulated
        emptyPrefixAndToken.value.accounts.map(_.accountId) shouldBe names
        emptyPrefixAndToken.value.nextPageToken shouldBe None
        // No next page token when the last page is exactly full
        exactlyFull.value.accounts.map(_.accountId) shouldBe Seq("acme/a", "acme/b", "acme/c")
        exactlyFull.value.nextPageToken shouldBe None
        allExactlyFull.value.accounts.map(_.accountId) shouldBe names
        allExactlyFull.value.nextPageToken shouldBe None
      }
    }

    "page past accounts stored with a legacy-format ID" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = newService(store, clock)

      // "b%" is a legacy valid Account ID
      val accounts = Seq("a", "b%", "c").map(AccountId.fromLegacyFormat(_).value)

      for {
        _ <- FutureUnlessShutdown.sequence(accounts.zipWithIndex.map { case (account, idx) =>
          store
            .persistTrafficDelta(
              account,
              EventId.tryCreate(s"event-$idx"),
              EventSource.TeaAPI,
              TrafficDelta.creditBalanceDelta(10),
              clock.now,
            )
            .valueOrFail("persist")
        })
        page1 <- service.listAccounts(listRequest(pageSize = Some(2)))
        page2 <- service.listAccounts(
          listRequest(pageSize = Some(2), pageToken = page1.value.nextPageToken)
        )
      } yield {
        page1.value.accounts.map(_.accountId) shouldBe Seq("a", "b%")
        page1.value.nextPageToken should not be empty
        page2.value.accounts.map(_.accountId) shouldBe Seq("c")
        page2.value.nextPageToken shouldBe None
      }
    }

    "reject malformed requests" in {
      def encodeToken(accountId: String): Option[String] =
        Some(
          Base64.getUrlEncoder.encodeToString(
            ListAccountsPageTokenPayload(accountIdLowerBoundExcl = accountId).toByteArray
          )
        )

      val clock = new SimClock(loggerFactory = loggerFactory)
      val service = newService(new TeaMemoryTrafficStore(loggerFactory), clock)

      for {
        zeroPageSize <- service.listAccounts(listRequest(pageSize = Some(0)))
        negativePageSize <- service.listAccounts(listRequest(pageSize = Some(-1)))
        tooLargePageSize <- service.listAccounts(
          listRequest(pageSize = Some(TrafficEnforcementService.MaxListAccountsPageSize + 1))
        )
        badPrefix <- service.listAccounts(listRequest(accountIdPrefix = Some("acme%")))
        longPrefix <- service.listAccounts(listRequest(accountIdPrefix = Some("a" * 256)))
        longToken <- service.listAccounts(listRequest(pageToken = encodeToken("a" * 256)))
        notBase64Token <- service.listAccounts(listRequest(pageToken = Some("acme/c")))
        notProtoToken <- service.listAccounts(
          // No payload token proto wrapper
          listRequest(pageToken = Some(Base64.getUrlEncoder.encodeToString(Array(0xff.toByte))))
        )
      } yield forEvery(
        Seq(
          zeroPageSize,
          negativePageSize,
          tooLargePageSize,
          badPrefix,
          longPrefix,
          longToken,
          notBase64Token,
          notProtoToken,
        )
      )(inside(_) { case Left(error) => error.code.id shouldBe ProtoDeserializationFailure.id })
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
      val malformedAccountId = "acme//billing"

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
        malformedAccount <- service.registerSubmission(
          RegisterSubmissionRequest(
            key.changeIdHash.toHexString,
            key.submissionId,
            Some(ChargedAccount(malformedAccountId, None)),
          )
        )
        malformedGet <- service.getAccount(GetAccountRequest(malformedAccountId))
        malformedUpdate <- service.updateAccount(
          UpdateAccountRequest(malformedAccountId, Some(1L), "update")
        )
        malformedNoOpUpdate <- service.updateAccount(
          UpdateAccountRequest(malformedAccountId, None, "update")
        )
        stored <- store.lookupCorrelation(key).valueOrFail("lookup")
        balance <- store
          .getBalance(AccountId(String255.tryCreate(malformedAccountId)))
          .valueOrFail("balance")
      } yield {
        forEvery(
          Seq(
            badHash,
            noSubmissionId,
            emptyAccount,
            malformedAccount,
            malformedGet,
            malformedUpdate,
            malformedNoOpUpdate,
          )
        ) { result =>
          inside(result) { case Left(error) =>
            error.code.id shouldBe ProtoDeserializationFailure.id
          }
        }
        stored shouldBe None
        balance shouldBe None
      }
    }
  }

  private def listRequest(
      pageSize: Option[Int] = None,
      pageToken: Option[String] = None,
      accountIdPrefix: Option[String] = None,
  ): ListAccountsRequest = ListAccountsRequest(pageSize, pageToken, accountIdPrefix)
}
