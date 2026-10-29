// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tea.TrafficEnforcementErrors.{InsufficientBalance, UnsafeToPrune}
import com.digitalasset.canton.tea.projection.{
  CorrelationAccount,
  CorrelationKey,
  EventId,
  EventSource,
  SubmissionCorrelation,
  TeaTrafficStore,
  TrafficDelta,
}
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  PruneRequest,
  PruneResponse,
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Transport-agnostic Traffic Enforcement App (TEA) operations.
  */
class TrafficEnforcementService(
    store: TeaTrafficStore,
    clock: Clock,
    minimumPruningRetention: NonNegativeFiniteDuration,
    metrics: TrafficEnforcementMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Return the local accounts (and their balances) configured for the requested account ID. */
  def getAccount(request: GetAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[RpcError, GetAccountResponse]] = {

    val result = for {
      accountId <- EitherT
        .fromEither[FutureUnlessShutdown](
          AccountId
            .fromProtoPrimitive(request.accountId, "account_id")
            .leftMap[RpcError](ProtoDeserializationFailure.Wrap(_))
        )
      balance <- store.getBalance(accountId).leftWiden[RpcError]
    } yield {
      balance match {
        case Some(value) => GetAccountResponse(value.account.str.unwrap, value.balance)
        // Returning balance 0L if the account is unknown
        case None => GetAccountResponse(request.accountId, 0L)
      }
    }

    result.value
  }

  /** Record which account a submission is charged to, after checking its balance if requested.
    *
    * The balance check and the insert aren't atomic, so concurrent submissions can both pass it.
    */
  def registerSubmission(request: RegisterSubmissionRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[RpcError, RegisterSubmissionResponse]] = {
    def parsed[A](result: ParsingResult[A]): EitherT[FutureUnlessShutdown, RpcError, A] =
      EitherT.fromEither[FutureUnlessShutdown](
        result.leftMap[RpcError](ProtoDeserializationFailure.Wrap(_))
      )

    val result = for {
      changeIdHash <- parsed(
        LfHash
          .fromString(request.changeIdHash)
          .leftMap(ProtoDeserializationError.ValueConversionError("change_id_hash", _))
      )
      submissionId <- parsed(
        Either.cond(
          request.submissionId.nonEmpty,
          request.submissionId,
          ProtoDeserializationError.FieldNotSet("submission_id"),
        )
      )
      accountO <- parsed(request.account.traverse { charged =>
        // We check this first so an empty id gets a clearer error.
        Either
          .cond(
            charged.accountId.nonEmpty,
            charged.accountId,
            ProtoDeserializationError.ValueConversionError("account_id", "must not be empty"),
          )
          .flatMap(AccountId.fromProtoPrimitive(_, "account_id"))
      })
      _ <- (accountO, request.account.flatMap(_.requiredBalance)) match {
        case (Some(account), Some(requiredBalance)) =>
          store.getBalance(account).leftWiden[RpcError].flatMap { stateO =>
            val balance = stateO.fold(0L)(_.balance)
            EitherT.cond[FutureUnlessShutdown](
              balance >= requiredBalance,
              (),
              InsufficientBalance.Reject(account.unwrap, balance, requiredBalance): RpcError,
            )
          }
        case _ => EitherT.rightT[FutureUnlessShutdown, RpcError](())
      }
      _ <- store
        .insertCorrelation(
          SubmissionCorrelation(
            CorrelationKey(changeIdHash, submissionId),
            CorrelationAccount.fromOption(accountO),
            clock.now,
          )
        )
        .leftWiden[RpcError]
    } yield RegisterSubmissionResponse()

    result.value
  }

  /** Update the account state for the given account ID */
  def updateAccount(request: UpdateAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[RpcError, UpdateAccountResponse]] = {
    def processBalanceDelta(accountId: AccountId, balanceDelta: Long) = for {
      eventId <- EitherT.fromEither[FutureUnlessShutdown](
        EventId
          .fromProtoPrimitive(request.deduplicationId)
          .leftMap[RpcError](ProtoDeserializationFailure.Wrap(_))
      )
      newBalance <- store
        .persistTrafficDelta(
          accountId,
          eventId,
          EventSource.TeaAPI,
          // Use TrafficDelta.creditDelta regardless of the sign of balanceDelta, on purpose. This endpoint allows to move the
          // "totalCredit" value on the balance "up and down". It does not change the "totalDebit"
          // value, which is updated when traffic is consumed on Ledger and attributed to this account.
          // This also avoids getting the account stuck: if the absolute value of `balanceDelta` was added
          // to either "totalCredit" or "totalDebit" depending on its sign,
          // an `updateAccount(Long.MaxValue)` followed by a `updateAccount(-Long.MaxValue)` would max out both
          // totals and prevent further updates to the account
          TrafficDelta.creditBalanceDelta(balanceDelta),
          clock.now,
        )
        .leftWiden[RpcError]
    } yield {
      newBalance match {
        case Some(accountState) =>
          UpdateAccountResponse(
            Some(GetAccountResponse(accountState.account.unwrap, accountState.balance))
          )
        case None =>
          UpdateAccountResponse(None)
      }
    }

    val result = for {
      accountId <- EitherT.fromEither[FutureUnlessShutdown](
        AccountId
          .fromProtoPrimitive(request.accountId, "account_id")
          .leftMap[RpcError](ProtoDeserializationFailure.Wrap(_))
      )
      response <- request.balanceDelta match {
        case Some(balanceDelta) => processBalanceDelta(accountId, balanceDelta)
        case None => EitherT.rightT[FutureUnlessShutdown, RpcError](UpdateAccountResponse(None))
      }
    } yield response

    result.value
  }

  def prune(request: PruneRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[RpcError, PruneResponse]] = {

    val result: EitherT[FutureUnlessShutdown, RpcError, PruneResponse] = for {
      ts <- EitherT.fromEither[FutureUnlessShutdown](
        ProtoConverter
          .parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "before_or_at",
            request.beforeOrAt,
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      safeBeforeOrAt = clock.now.minus(minimumPruningRetention.asJava)
      _ <- EitherT.cond[FutureUnlessShutdown](
        ts <= safeBeforeOrAt,
        (),
        UnsafeToPrune.Reject(ts, safeBeforeOrAt, minimumPruningRetention): RpcError,
      )
      pruneResult <- store.prune(ts).leftWiden[RpcError]
    } yield {
      metrics.markPrunedRows("events", pruneResult.events)
      metrics.markPrunedRows("correlations", pruneResult.correlations)
      metrics.markPrunedRows("uncharged_completions", pruneResult.unchargedCompletions)
      PruneResponse(
        prunedEventCount = pruneResult.events,
        prunedCorrelationCount = pruneResult.correlations,
        prunedUnchargedCompletionCount = pruneResult.unchargedCompletions,
      )
    }

    result.value
  }
}
