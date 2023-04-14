// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Hash}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.v2.TopologyMappingX.Mapping
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v1
import com.digitalasset.canton.topology.admin.v1.AuthorizeRequest.{Proposal, Type}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyManagerWriteServiceX(
    manager: TopologyManagerX,
    topologyStoreX: TopologyStoreX[AuthorizedStore],
    getId: => Option[UniqueIdentifier],
    crypto: Crypto,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v1.TopologyManagerWriteServiceXGrpc.TopologyManagerWriteServiceX
    with NamedLogging {

  override def authorize(request: v1.AuthorizeRequest): Future[v1.AuthorizeResponse] = {
    implicit val traceContext: TraceContext = TraceContext.todo

    val result = request.`type` match {
      case Type.Empty =>
        EitherT.leftT[Future, GenericSignedTopologyTransactionX][CantonError](
          ProtoDeserializationFailure.Wrap(
            ProtoDeserializationError.FieldNotSet("AuthorizeRequest.type")
          )
        )

      case Type.TransactionHash(value) =>
        for {
          txHash <- EitherT
            .fromEither[Future](Hash.fromHexString(value).map(TxHash))
            .leftMap(err => ProtoDeserializationFailure.Wrap(err.toProtoDeserializationError))
          signingKeys <-
            EitherT
              .fromEither[Future](request.signedBy.traverse(Fingerprint.fromProtoPrimitive))
              .leftMap(ProtoDeserializationFailure.Wrap(_))
          signedTopoTx <-
            // TODO(#11255) understand when and why force needs to come in effect
            manager
              .accept(
                txHash,
                signingKeys,
                force = false,
                expectFullAuthorization = request.mustFullyAuthorize,
              )
              .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }

      case Type.Proposal(Proposal(op, serial, mapping)) =>
        val validatedMappingE = for {
          serial <- ProtoConverter.parsePositiveInt(serial)
          op <- TopologyChangeOpX.fromProtoV2(op)
          mapping <- ProtoConverter.required("AuthorizeRequest.mapping", mapping)
          signingKeys <-
            request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
          validatedMapping <- mapping.mapping match {
            case Mapping.NamespaceDelegation(mapping) =>
              NamespaceDelegationX.fromProtoV2(mapping)
            case Mapping.UnionspaceDefinition(mapping) =>
              UnionspaceDefinitionX.fromProtoV2(mapping)
            case Mapping.DomainParametersState(mapping) =>
              DomainParametersStateX.fromProtoV2(mapping)
            case Mapping.MediatorDomainState(mapping) =>
              MediatorDomainStateX.fromProtoV2(mapping)
            case Mapping.SequencerDomainState(mapping) =>
              SequencerDomainStateX.fromProtoV2(mapping)
            case _ =>
              // TODO(#11255): match missing cases
              ???
          }
        } yield {
          (op, serial, validatedMapping, signingKeys, request.forceChange)
        }
        for {
          mapping <- EitherT
            .fromEither[Future](validatedMappingE)
            .leftMap(ProtoDeserializationFailure.Wrap(_))
          (op, serial, validatedMapping, signingKeys, forceChange) = mapping
          signedTopoTx <- manager
            .proposeAndAuthorize(
              op,
              validatedMapping,
              Some(serial),
              signingKeys,
              protocolVersion,
              expectFullAuthorization = request.mustFullyAuthorize,
              force = forceChange,
            )
            .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }
    }
    CantonGrpcUtil.mapErrNew(result).map(tx => v1.AuthorizeResponse(Some(tx.toProtoV2)))
  }

  override def addTransactions(
      request: v1.AddTransactionsRequest
  ): Future[v1.AddTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      signedTxs <- EitherT.fromEither[Future](
        request.transactions
          .traverse(SignedTopologyTransactionX.fromProtoV2)
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      // TODO(#11255) let the caller decide whether to expect full authorization or not?
      _ <- manager
        .add(signedTxs, force = request.forceChange, expectFullAuthorization = false)
        .leftWiden[CantonError]
    } yield v1.AddTransactionsResponse()
    CantonGrpcUtil.mapErrNew(res).andThen(_ => topologyStoreX.dumpStoreContent())
  }

}
