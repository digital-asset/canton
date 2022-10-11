// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.participant.admin.v0.{
  PartyNameManagementServiceGrpc,
  SetPartyDisplayNameRequest,
  SetPartyDisplayNameResponse,
}
import com.digitalasset.canton.participant.topology.LedgerServerPartyNotifier
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class GrpcPartyNameManagementService(notifier: LedgerServerPartyNotifier)(implicit
    ec: ExecutionContext
) extends PartyNameManagementServiceGrpc.PartyNameManagementService {
  override def setPartyDisplayName(
      request: SetPartyDisplayNameRequest
  ): Future[SetPartyDisplayNameResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      (for {
        partyId <- EitherT.fromEither[Future](
          UniqueIdentifier
            .fromProtoPrimitive(request.partyId, "partyId")
            .map(PartyId(_))
            .leftMap(x => x.toString)
        )
        // validating displayName length here and not on client-side
        displayName <- EitherT.fromEither[Future](String255.create(request.displayName))
        _ <- EitherT.rightT[Future, String](notifier.setDisplayName(partyId, displayName))
      } yield SetPartyDisplayNameResponse()).value.transform {
        case Success(Left(err)) =>
          Failure(Status.INVALID_ARGUMENT.withDescription(err).asRuntimeException())
        case Success(Right(v)) => Success(v)
        case Failure(x) => Failure(x)
      }
    }
}
