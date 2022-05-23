// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{mapErrNew, wrapErr}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.{v0 => adminProto}
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.HasProtoV0
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

case class BaseQuery(
    filterStore: String,
    useStateStore: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOp],
    filterSigningKey: String,
) extends HasProtoV0[adminProto.BaseQuery] {
  override def toProtoV0: adminProto.BaseQuery =
    adminProto.BaseQuery(
      filterStore,
      useStateStore,
      ops.map(_.toProto).getOrElse(TopologyChangeOp.Add.toProto),
      ops.nonEmpty,
      timeQuery.toProtoV0,
      filterSigningKey,
    )
}

object BaseQuery {
  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQuery] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      filterStore = baseQuery.filterStore
      useStateStore = baseQuery.useStateStore
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQuery.fromProto(baseQuery.timeQuery, "time_query")
      opsRaw <- TopologyChangeOp.fromProtoV0(baseQuery.operation)
    } yield BaseQuery(
      filterStore,
      useStateStore,
      timeQuery,
      if (baseQuery.filterOperation) Some(opsRaw) else None,
      filterSignedKey,
    )
}

/** GRPC service implementation for deep topology transaction inspection
  *
  * This service is mostly used for debugging and transaction exporting purposes.
  *
  * @param stores the various identity stores
  */
class GrpcTopologyManagerReadService(
    stores: => Future[Seq[TopologyStore[TopologyStoreId]]],
    ips: IdentityProvidingServiceClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends adminProto.TopologyManagerReadServiceGrpc.TopologyManagerReadService
    with NamedLogging {

  private case class TransactionSearchResult(
      store: TopologyStoreId,
      sequenced: SequencedTime,
      validFrom: EffectiveTime,
      validUntil: Option[EffectiveTime],
      operation: TopologyChangeOp,
      serialized: ByteString,
      signedBy: Fingerprint,
  )

  private def collectStores(
      filterStore: String
  ): EitherT[Future, CantonError, Seq[TopologyStore[TopologyStoreId]]] = {
    val res = stores.map { allStores =>
      allStores.filter(_.storeId.filterName.startsWith(filterStore))
    }
    EitherT.right(res)
  }

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult =
    new adminProto.BaseResult(
      store = context.store.filterName,
      sequenced = Some(context.sequenced.value.toProtoPrimitive),
      validFrom = Some(context.validFrom.value.toProtoPrimitive),
      validUntil = context.validUntil.map(_.value.toProtoPrimitive),
      operation = context.operation.toProto,
      serialized = context.serialized,
      signedByFingerprint = context.signedBy.unwrap,
    )

  // to avoid race conditions, we want to use the approximateTimestamp of the topology client.
  // otherwise, we might read stuff from the database that isn't yet known to the node
  private def getApproximateTimestamp(storeId: TopologyStoreId): Option[CantonTimestamp] =
    storeId match {
      case DomainStore(domainId, _) =>
        ips.forDomain(domainId).map(_.approximateTimestamp)
      case _ => None
    }

  private def collectFromStores(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: DomainTopologyTransactionType,
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, Seq[(TransactionSearchResult, TopologyMapping)]] = {

    def fromStore(
        baseQuery: BaseQuery,
        store: TopologyStore[TopologyStoreId],
    ): Future[Seq[(TransactionSearchResult, TopologyMapping)]] = {
      val storeId = store.storeId

      store
        .inspect(
          stateStore =
            // the topology manager does not write to the state store, so a state store query does not make sense
            if (
              storeId == TopologyStoreId.AuthorizedStore || storeId == TopologyStoreId.RequestedStore
            )
              false
            else baseQuery.useStateStore,
          timeQuery = baseQuery.timeQuery,
          recentTimestampO = getApproximateTimestamp(storeId),
          ops = baseQuery.ops,
          typ = Some(typ),
          idFilter = idFilter,
          namespaceOnly,
        )
        .map { col =>
          col.result.collect {
            case tx
                if tx.transaction.key.fingerprint.unwrap.startsWith(baseQuery.filterSigningKey) =>
              val result = TransactionSearchResult(
                storeId,
                tx.sequenced,
                tx.validFrom,
                tx.validUntil,
                tx.transaction.operation,
                tx.transaction.getCryptographicEvidence,
                tx.transaction.key.fingerprint,
              )
              (result, tx.transaction.transaction.element.mapping)
          }
        }
    }
    for {
      baseQuery <- wrapErr(BaseQuery.fromProto(baseQueryProto))
      stores <- collectStores(baseQuery.filterStore)
      results <- EitherT.right(stores.traverse { store =>
        fromStore(baseQuery, store)
      })
    } yield {
      val res = results.flatten
      if (baseQuery.filterSigningKey.nonEmpty)
        res.filter(x => x._1.signedBy.unwrap.startsWith(baseQuery.filterSigningKey))
      else res
    }
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        filterRequestSide <- wrapErr(
          request.filterRequestSide.map(_.value).traverse(RequestSide.fromProtoEnum)
        )
        filterPermission <- wrapErr(
          request.filterPermission.map(_.value).traverse(ParticipantPermission.fromProtoEnum)
        )
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.PartyToParticipant,
          request.filterParty,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect {
            case (result, x: PartyToParticipant)
                if x.party.filterString.startsWith(
                  request.filterParty
                ) && x.participant.filterString.startsWith(request.filterParticipant)
                  && filterRequestSide
                    .forall(_ == x.side) && filterPermission.forall(_ == x.permission) =>
              (result, x)
          }
          .map { case (context, elem) =>
            new adminProto.ListPartyToParticipantResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV0),
            )
          }
        adminProto.ListPartyToParticipantResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        filterKeyPurpose <- wrapErr(
          request.filterKeyPurpose.traverse(x =>
            KeyPurpose.fromProtoEnum("filterKeyPurpose", x.value)
          )
        )
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.OwnerToKeyMapping,
          request.filterKeyOwnerUid,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect {
            case (result, x: OwnerToKeyMapping)
                if x.owner.filterString.startsWith(request.filterKeyOwnerUid) &&
                  (request.filterKeyOwnerType.isEmpty || request.filterKeyOwnerType == x.owner.code.threeLetterId.unwrap) &&
                  (filterKeyPurpose.isEmpty || filterKeyPurpose.contains(x.key.purpose)) =>
              (result, x)
          }
          .map { case (context, elem) =>
            new adminProto.ListOwnerToKeyMappingResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV0),
              keyFingerprint = elem.key.fingerprint.unwrap,
            )
          }
        adminProto.ListOwnerToKeyMappingResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listNamespaceDelegation(
      request: adminProto.ListNamespaceDelegationRequest
  ): Future[adminProto.ListNamespaceDelegationResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.NamespaceDelegation,
          request.filterNamespace,
          namespaceOnly = true,
        )
      } yield {
        val results = res
          .collect { case (result, x: NamespaceDelegation) =>
            (result, x)
          }
          .map { case (context, elem) =>
            new adminProto.ListNamespaceDelegationResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV0),
              targetKeyFingerprint = elem.target.fingerprint.unwrap,
            )
          }

        adminProto.ListNamespaceDelegationResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listIdentifierDelegation(
      request: adminProto.ListIdentifierDelegationRequest
  ): Future[adminProto.ListIdentifierDelegationResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.IdentifierDelegation,
          request.filterUid,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect { case (result, x: IdentifierDelegation) =>
            (result, x)
          }
          .map { case (context, elem) =>
            new adminProto.ListIdentifierDelegationResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV0),
              targetKeyFingerprint = elem.target.fingerprint.unwrap,
            )
          }
        adminProto.ListIdentifierDelegationResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResult] =
    stores.map { allStores =>
      adminProto.ListAvailableStoresResult(storeIds = allStores.map(_.storeId.filterName))
    }

  override def listParticipantDomainState(
      request: adminProto.ListParticipantDomainStateRequest
  ): Future[adminProto.ListParticipantDomainStateResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.ParticipantState,
          request.filterParticipant,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect {
            case (context, elem: ParticipantState)
                if elem.domain.filterString.startsWith(request.filterDomain) =>
              new adminProto.ListParticipantDomainStateResult.Result(
                context = Some(createBaseResult(context)),
                item = Some(elem.toProtoV0),
              )
          }
        adminProto.ListParticipantDomainStateResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listSignedLegalIdentityClaim(
      request: adminProto.ListSignedLegalIdentityClaimRequest
  ): Future[adminProto.ListSignedLegalIdentityClaimResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.SignedLegalIdentityClaim,
          request.filterUid,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect { case (result, x: SignedLegalIdentityClaim) =>
            (result, x)
          }
          .map { case (context, elem) =>
            new adminProto.ListSignedLegalIdentityClaimResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV0),
            )
          }
        adminProto.ListSignedLegalIdentityClaimResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  /** Access all topology transactions
    */
  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] =
    fromGrpcContext { implicit traceContext =>
      val res = for {
        baseQuery <- wrapErr(BaseQuery.fromProto(request.baseQuery))
        stores <- collectStores(baseQuery.filterStore)
        results <- EitherT.right(
          stores.traverse { store =>
            store
              .inspect(
                stateStore =
                  // state store doesn't make any sense for the authorized store
                  if (store.storeId == TopologyStoreId.AuthorizedStore) false
                  else baseQuery.useStateStore,
                timeQuery = baseQuery.timeQuery,
                recentTimestampO = getApproximateTimestamp(store.storeId),
                ops = baseQuery.ops,
                typ = None,
                idFilter = "",
                namespaceOnly = false,
              )
          }
        ): EitherT[Future, CantonError, Seq[StoredTopologyTransactions[TopologyChangeOp]]]
      } yield {
        val res = results.foldLeft(StoredTopologyTransactions.empty[TopologyChangeOp]) {
          case (acc, elem) =>
            StoredTopologyTransactions(
              acc.result ++ elem.result.filter(
                _.transaction.key.fingerprint.unwrap.startsWith(baseQuery.filterSigningKey)
              )
            )
        }
        adminProto.ListAllResponse(result = Some(res.toProtoV0))
      }
      EitherTUtil.toFuture(mapErrNew(res))
    }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResult] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.PackageUse,
          request.filterParticipant,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect { case (context, vetted: VettedPackages) =>
            new adminProto.ListVettedPackagesResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(vetted.toProtoV0),
            )
          }
        adminProto.ListVettedPackagesResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
    }

  override def listDomainParametersChanges(
      request: adminProto.ListDomainParametersChangesRequest
  ): Future[adminProto.ListDomainParametersChangesResult] = TraceContext.fromGrpcContext {
    implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.DomainParameters,
          "",
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect { case (context, domainParametersChange: DomainParametersChange) =>
            adminProto.ListDomainParametersChangesResult.Result(
              Some(createBaseResult(context)),
              Some(domainParametersChange.domainParameters.toProtoV0),
            )
          }

        adminProto.ListDomainParametersChangesResult(results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
  }

  override def listMediatorDomainState(
      request: adminProto.ListMediatorDomainStateRequest
  ): Future[adminProto.ListMediatorDomainStateResult] = TraceContext.fromGrpcContext {
    implicit traceContext =>
      val ret = for {
        res <- collectFromStores(
          request.baseQuery,
          DomainTopologyTransactionType.MediatorDomainState,
          request.filterMediator,
          namespaceOnly = false,
        )
      } yield {
        val results = res
          .collect {
            case (context, elem: MediatorDomainState)
                if elem.domain.filterString.startsWith(request.filterDomain) =>
              new adminProto.ListMediatorDomainStateResult.Result(
                context = Some(createBaseResult(context)),
                item = Some(elem.toProtoV0),
              )
          }
        adminProto.ListMediatorDomainStateResult(results = results)
      }
      EitherTUtil.toFuture(mapErrNew(ret))
  }

}
