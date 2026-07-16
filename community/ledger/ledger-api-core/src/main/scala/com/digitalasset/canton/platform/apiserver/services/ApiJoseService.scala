// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.ledger.api.v2.jose_service.JoseServiceGrpc.JoseService
import com.daml.ledger.api.v2.jose_service.{GetJwksRequest, GetJwksResponse, JoseServiceGrpc}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningKeyUsage}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcETFUSExtended
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContextGrpc
import io.circe.syntax.*
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiJoseService(
    pureCrypto: CryptoPureApi,
    lookupTopologyClient: SynchronizerId => Option[SynchronizerTopologyClient],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends JoseService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  override def bindService(): ServerServiceDefinition =
    JoseServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def getJwks(request: GetJwksRequest): Future[GetJwksResponse] =
    withEnrichedLoggingContext(TraceContextGrpc.fromGrpcContext)(
      logging.partyString(request.partyId)
    ) { implicit loggingContext =>
      val response: EitherT[FutureUnlessShutdown, StatusRuntimeException, GetJwksResponse] = for {
        synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
          SynchronizerId
            .fromProtoPrimitive(request.synchronizerId, "synchronizerId")
            .leftMap(err => RequestValidationErrors.InvalidArgument.Reject(err.message).asGrpcError)
        )
        partyId <- EitherT.fromEither[FutureUnlessShutdown](
          PartyId
            .fromProtoPrimitive(request.partyId, "partyId")
            .leftMap(err => RequestValidationErrors.InvalidArgument.Reject(err.message).asGrpcError)
        )
        topologyClient <- EitherT.fromEither[FutureUnlessShutdown](
          lookupTopologyClient(synchronizerId).toRight(
            RequestValidationErrors.NotFound.Synchronizer.Reject(request.synchronizerId).asGrpcError
          )
        )
        snapshot <- EitherT.liftF(topologyClient.currentSnapshotApproximation)
        foundKeys <- EitherT(
          snapshot
            .signingKeysWithThreshold(partyId)
            .map(
              _.toRight(
                RequestValidationErrors.NotFound.PartySigningKeys
                  .Reject(request.partyId)
                  .asGrpcError
              )
            )
        )
        usableKeys = foundKeys.keys.forgetNE.toList.filter { key =>
          // Currently, we allow Party JWTs to be signed with the same keys that are used to sign protocol messages.
          // TODO(i32231): Introduce a dedicated key usage for this, but keep Protocol for backwards-compatibility with older keys.
          key.usage.contains(SigningKeyUsage.Protocol)
        }
        jwksKeys <- EitherT.fromEither[FutureUnlessShutdown](
          usableKeys.traverse { key =>
            pureCrypto
              .toJwk(key)
              .leftMap(err =>
                LedgerApiErrors.InternalError.Generic(s"jwk conversion failed: $err").asGrpcError
              )
          }
        )
      } yield GetJwksResponse(
        keys = jwksKeys.map(_.toJsonObject.asJson.noSpaces)
      )

      response.asGrpcResponse
    }
}
