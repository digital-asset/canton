// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Hash}
import com.digitalasset.canton.environment.CantonNodeBootstrapX
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.protocol.v2.TopologyMappingX.Mapping
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v1
import com.digitalasset.canton.topology.admin.v1.AuthorizeRequest.{Proposal, Type}
import com.digitalasset.canton.topology.admin.v1.{
  GenerateGenesisTopologyTransactionsRequest,
  GenerateGenesisTopologyTransactionsResponse,
}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyManagerWriteServiceX(
    manager: TopologyManagerX,
    topologyStoreX: TopologyStoreX[AuthorizedStore],
    cantonNodeBootstrapX: CantonNodeBootstrapX[_, _, _, _],
    crypto: Crypto,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v1.TopologyManagerWriteServiceXGrpc.TopologyManagerWriteServiceX
    with NamedLogging {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*

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
          serial <- PositiveInt
            .create(serial)
            .leftMap(ProtoDeserializationError.InvariantViolation(_))
          op <-
            TopologyChangeOpX.fromProtoV2(op)
          mapping <- ProtoConverter.required("AuthorizeRequest.mapping", mapping)
          signingKeys <-
            request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
          validatedMapping <- mapping.mapping match {
            case Mapping.NamespaceDelegation(namespaceDelegationX) =>
              NamespaceDelegationX.fromProtoV2(namespaceDelegationX)
            case Mapping.UnionspaceDefinition(unionspaceDefinitionX) =>
              UnionspaceDefinitionX.fromProtoV2(unionspaceDefinitionX)
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
    EitherTUtil.toFuture(mapErrNew(result)).map(tx => v1.AuthorizeResponse(Some(tx.toProtoV2)))
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
    EitherTUtil
      .toFuture(mapErrNew(res))
      .andThen({ case _ =>
        topologyStoreX.dumpStoreContent()
      })
  }

  override def generateGenesisTopologyTransactions(
      request: GenerateGenesisTopologyTransactionsRequest
  ): Future[GenerateGenesisTopologyTransactionsResponse] = {
    // TODO(#11255) proper trace context
    implicit val traceContext = TraceContext.todo
    val effective = EffectiveTime(clock.now)

    // TODO(#11255) do proper request validation
    val result = for {
      parsedMembers <- EitherT
        .fromEither[Future](
          request.foundingMembers
            .traverse(Member.fromProtoPrimitive(_, "founding_members"))
        )
        .leftMap[CantonError](ProtoDeserializationFailure.Wrap(_))

      members <-
        EitherT.fromOption[Future][CantonError, NonEmpty[Seq[Member]]](
          NonEmpty.from(parsedMembers),
          ProtoDeserializationFailure.Wrap(
            ProtoDeserializationError.FieldNotSet("founding_members")
          ),
        )

      sequencers = members.collect { case s @ SequencerId(_) => s }
      mediators = members.collect { case m @ MediatorId(_) => m }

      memberNamespaces = members.map(_.uid.namespace)
      domainName = Identifier.tryCreate(request.domainName)
      domainNamespace =
        UnionspaceDefinitionX.computeNamespace(crypto.pureCrypto, memberNamespaces.toSet)
      domainId = DomainId(UniqueIdentifier(domainName, domainNamespace))

      thisNodeId <- EitherT
        .fromOption[Future][CantonError, UniqueIdentifier](
          cantonNodeBootstrapX.getId.map(_.identity),
          TopologyManagerError.InternalError.ImplementMe(),
        )
        .leftWiden[CantonError]
      namespaceTx <- EitherT
        .right[CantonError](
          topologyStoreX.findValidTransactions(
            effective,
            _.transaction.mapping
              .as[NamespaceDelegationX]
              .exists(ns => ns.namespace == thisNodeId.namespace && ns.isRootDelegation),
          )
        )
      keysToUse = namespaceTx
        .flatMap(_.transaction.mapping.as[NamespaceDelegationX])
        .map(_.target.id)

      otkTx <- EitherT
        .right[CantonError](
          topologyStoreX.findValidTransactions(
            effective,
            _.transaction.mapping.as[OwnerToKeyMappingX].exists(_.member.uid == thisNodeId),
          )
        )
      usDef <- EitherT
        .fromEither[Future](
          UnionspaceDefinitionX.create(
            domainNamespace,
            PositiveInt.tryCreate(members.size - 1),
            memberNamespaces.forgetNE.toSeq,
          )
        )
        .leftMap[CantonError](_ => TopologyManagerError.InternalError.ImplementMe())

      unionspaceTx <- manager
        .proposeAndAuthorize(
          TopologyChangeOpX.Replace,
          usDef,
          None,
          signingKeys = keysToUse,
          protocolVersion,
          expectFullAuthorization = false,
        )
        .leftWiden[CantonError]

      domainParamsTx <- manager
        .proposeAndAuthorize(
          TopologyChangeOpX.Replace,
          DomainParametersStateX(domainId)(
            DynamicDomainParameters.defaultValues(protocolVersion)
          ),
          serial = None,
          signingKeys = keysToUse,
          protocolVersion,
          expectFullAuthorization = false,
        )
        .leftWiden[CantonError]

      medState <- EitherT
        .fromEither[Future](
          MediatorDomainStateX.create(
            domainId,
            NonNegativeInt.one,
            PositiveInt.tryCreate(mediators.size - 1),
            active = mediators.toSeq,
            observers = Seq.empty,
          )
        )
        .leftMap[CantonError](_ => TopologyManagerError.InternalError.ImplementMe())
      medStateTx <- manager
        .proposeAndAuthorize(
          TopologyChangeOpX.Replace,
          medState,
          serial = None,
          signingKeys = keysToUse,
          protocolVersion,
          expectFullAuthorization = false,
        )
        .leftWiden[CantonError]

      seqState <- EitherT
        .fromEither[Future](
          SequencerDomainStateX.create(
            domainId,
            PositiveInt.tryCreate(sequencers.size - 1),
            active = sequencers.toSeq,
            observers = Seq.empty,
          )
        )
        .leftMap[CantonError](_ => TopologyManagerError.InternalError.ImplementMe())
      seqStateTx <- manager
        .proposeAndAuthorize(
          TopologyChangeOpX.Replace,
          seqState,
          serial = None,
          signingKeys = keysToUse,
          protocolVersion,
          expectFullAuthorization = false,
        )
        .leftWiden[CantonError]
    } yield namespaceTx ++ otkTx ++ Seq(
      unionspaceTx,
      domainParamsTx,
      medStateTx,
      seqStateTx,
    )

    EitherTUtil
      .toFuture(result.leftMap(_.asGrpcError))
      .map(txs => GenerateGenesisTopologyTransactionsResponse(txs.map(_.toProtoV2)))
  }
}
