// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tea.TrafficEnforcementService.{
  InvalidArgument,
  TrafficEnforcementServiceError,
}
import com.digitalasset.canton.tea.projection.{
  AccountId,
  AccountState,
  EventId,
  EventSource,
  TeaTrafficStore,
  TrafficDelta,
}
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
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
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Return the local accounts (and their balances) configured for the requested account ID. */
  def getAccount(request: GetAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[TrafficEnforcementServiceError, GetAccountResponse]] = {

    val result = for {
      accountId <- EitherT.fromEither[FutureUnlessShutdown](
        AccountId
          .fromProtoPrimitive(request.accountId)
          .leftMap(err => InvalidArgument(request.accountId, err.message))
      )
      balance <- EitherT
        .liftF[FutureUnlessShutdown, TrafficEnforcementServiceError, Option[AccountState]](
          store.getBalance(accountId).value
        )
    } yield {
      balance match {
        case Some(value) => GetAccountResponse(value.account.str.unwrap, value.balance)
        // Returning balance 0L if the account is unknown
        case None => GetAccountResponse(request.accountId, 0L)
      }
    }

    result.value
  }

  /** Update the account state for the given account ID */
  def updateAccount(request: UpdateAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[TrafficEnforcementServiceError, UpdateAccountResponse]] = {
    def processBalanceDelta(balanceDelta: Long) = for {
      accountId <- EitherT.fromEither[FutureUnlessShutdown](
        AccountId
          .fromProtoPrimitive(request.accountId)
          .leftMap(err => InvalidArgument(request.accountId, err.message))
      )
      eventId <- EitherT.fromEither[FutureUnlessShutdown](
        EventId
          .fromProtoPrimitive(request.deduplicationId)
          .leftMap(err => InvalidArgument(request.deduplicationId, err.message))
      )
      newBalance <- EitherT
        .liftF[FutureUnlessShutdown, TrafficEnforcementServiceError, Option[AccountState]](
          store
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
            .value
        )
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

    request.balanceDelta match {
      case Some(balanceDelta) =>
        processBalanceDelta(balanceDelta).value
      case None =>
        FutureUnlessShutdown.pure(Right(UpdateAccountResponse(None)))
    }
  }
}

object TrafficEnforcementService {
  sealed trait TrafficEnforcementServiceError
  final case class NotEnoughTraffic(account: String, balance: Long, cost: Long)
      extends TrafficEnforcementServiceError
  final case class InvalidArgument(provided: String, error: String)
      extends TrafficEnforcementServiceError
}
