// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.SyncStateInspection
import com.digitalasset.canton.participant.admin.v0.InspectionServiceGrpc.InspectionService
import com.digitalasset.canton.participant.admin.v0.{
  LookupContractDomain,
  LookupOffsetByIndex,
  LookupOffsetByTime,
  LookupTransactionDomain,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(syncStateInspection: SyncStateInspection)(implicit
    executionContext: ExecutionContext
) extends InspectionService {

  override def lookupContractDomain(
      request: LookupContractDomain.Request
  ): Future[LookupContractDomain.Response] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      // TODO(error handling): Properly treat malformed contract ID strings
      syncStateInspection
        .lookupContractDomain(request.contractId.map(LfContractId.assertFromString).toSet)
        .map { results =>
          LookupContractDomain.Response(
            results.map { case (contractId, alias) => contractId.coid -> alias.unwrap }
          )
        }
    }

  override def lookupTransactionDomain(
      request: LookupTransactionDomain.Request
  ): Future[LookupTransactionDomain.Response] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      LedgerTransactionId.fromString(request.transactionId) match {
        case Left(err) =>
          Future.failed(
            new IllegalArgumentException(
              s"""String "${request.transactionId}" doesn't parse as a transaction ID: $err"""
            )
          )
        case Right(txId) =>
          syncStateInspection.lookupTransactionDomain(txId).map { domainId =>
            LookupTransactionDomain.Response(
              domainId.fold(throw new StatusRuntimeException(Status.NOT_FOUND))(_.toProtoPrimitive)
            )
          }
      }
    }

  override def lookupOffsetByTime(
      request: LookupOffsetByTime.Request
  ): Future[LookupOffsetByTime.Response] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      request.timestamp.fold[Future[LookupOffsetByTime.Response]](
        Future.failed(new IllegalArgumentException(s"""Timestamp not specified"""))
      ) { ts =>
        CantonTimestamp.fromProtoPrimitive(ts) match {
          case Right(cantonTimestamp) =>
            syncStateInspection
              .getOffsetByTime(cantonTimestamp)
              .map(ledgerOffset =>
                LookupOffsetByTime.Response(ledgerOffset.fold("")(_.getAbsolute))
              )
          case Left(err) =>
            Future.failed(new IllegalArgumentException(s"""Failed to parse timestamp: $err"""))
        }
      }
    }

  override def lookupOffsetByIndex(
      request: LookupOffsetByIndex.Request
  ): Future[LookupOffsetByIndex.Response] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      if (request.index <= 0) {
        Future.failed(
          new IllegalArgumentException(s"""Index needs to be positive and not ${request.index}""")
        )
      } else {
        syncStateInspection
          .locateOffset(request.index)
          .map(
            _.fold(
              err =>
                throw new StatusRuntimeException(
                  Status.OUT_OF_RANGE.withDescription(s"""Failed to locate offset: $err""")
                ),
              ledgerOffset => LookupOffsetByIndex.Response(ledgerOffset.getAbsolute),
            )
          )
      }
    }
}
