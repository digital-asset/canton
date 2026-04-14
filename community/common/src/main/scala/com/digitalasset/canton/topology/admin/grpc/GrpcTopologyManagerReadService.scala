// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.implicits.catsSyntaxEitherId
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{mapErrNewEUS, wrapErrUS}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30.*
import com.digitalasset.canton.topology.admin.{grpc, v30 as adminProto}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStore,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, MonadUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, topology}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

final case class BaseQuery(
    store: Option[grpc.TopologyStoreId],
    proposals: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOp],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV1: adminProto.BaseQuery =
    adminProto.BaseQuery(
      store.map(_.toProtoV30),
      proposals,
      ops.map(_.toProto).getOrElse(v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED),
      timeQuery.toProtoV30,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitive),
    )
}

object BaseQuery {
  def apply(
      store: TopologyStoreId,
      proposals: Boolean,
      timeQuery: TimeQuery,
      ops: Option[TopologyChangeOp],
      filterSigningKey: String,
      protocolVersion: Option[ProtocolVersion],
  ): BaseQuery =
    BaseQuery(
      Some(store),
      proposals,
      timeQuery,
      ops,
      filterSigningKey,
      protocolVersion,
    )

  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQuery] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      proposals = baseQuery.proposals
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQuery.fromProto(baseQuery.timeQuery, "time_query")
      operationOp <- TopologyChangeOp.fromProtoV30(baseQuery.operation)
      protocolVersion <- baseQuery.protocolVersion.traverse(ProtocolVersion.fromProtoPrimitive(_))
      store <- baseQuery.store.traverse(
        grpc.TopologyStoreId.fromProtoV30(_, "store")
      )
    } yield BaseQuery(
      store,
      proposals,
      timeQuery,
      operationOp,
      filterSignedKey,
      protocolVersion,
    )
}

class GrpcTopologyManagerReadService(
    member: Member,
    stores: => Seq[topology.store.TopologyStore[topology.store.TopologyStoreId]],
    crypto: Crypto,
    topologyClientLookup: PhysicalSynchronizerId => Option[SynchronizerTopologyClient],
    timeTrackerLookup: PhysicalSynchronizerId => Option[SynchronizerTimeTracker],
    physicalSynchronizerIdLookup: PsidLookup,
    processingTimeout: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext, materializer: Materializer)
    extends adminProto.TopologyManagerReadServiceGrpc.TopologyManagerReadService
    with NamedLogging {

  private case class TransactionSearchResult(
      store: TopologyStoreId,
      sequenced: SequencedTime,
      validFrom: EffectiveTime,
      validUntil: Option[EffectiveTime],
      operation: TopologyChangeOp,
      transactionHash: ByteString,
      serial: PositiveInt,
      signedBy: NonEmpty[Set[Fingerprint]],
  )

  private def collectStores(
      storeO: Option[grpc.TopologyStoreId]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Seq[
    topology.store.TopologyStore[topology.store.TopologyStoreId]
  ]] =
    storeO match {
      case Some(store) =>
        EitherT.rightT(
          activePsidFor(store).toOption.toList.flatMap(targetStoreId =>
            stores.filter(_.storeId == targetStoreId)
          )
        )
      case None => EitherT.rightT(stores)
    }

  private def activePsidFor(
      grpcTopologyStoreId: grpc.TopologyStoreId
  )(implicit
      traceContext: TraceContext
  ): Either[RpcError, topology.store.TopologyStoreId] =
    grpcTopologyStoreId
      .toInternal(physicalSynchronizerIdLookup)
      .leftMap(TopologyManagerError.InvalidSynchronizer.Failure(_))

  private def collectSynchronizerStore(
      storeO: Option[grpc.TopologyStoreId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, topology.store.TopologyStore[
    topology.store.TopologyStoreId.SynchronizerStore
  ]] = {
    val synchronizerStores =
      storeO match {
        case Some(store) =>
          activePsidFor(store).flatMap { targetStoreInternal =>
            val synchronizerStores = stores
              .flatMap(
                topology.store.TopologyStoreId
                  .select[topology.store.TopologyStoreId.SynchronizerStore]
              )
              .filter(store => store.storeId == targetStoreInternal)
            synchronizerStores match {
              case Nil =>
                TopologyManagerError.TopologyStoreUnknown
                  .Failure(targetStoreInternal)
                  .asLeft
              case Seq(synchronizerStore) => synchronizerStore.asRight
              case multiple =>
                TopologyManagerError.InvalidSynchronizer
                  .MultipleSynchronizerStoresFound(multiple.map(_.storeId))
                  .asLeft
            }
          }

        case None =>
          val synchronizerStores = stores
            .flatMap(
              topology.store.TopologyStoreId
                .select[topology.store.TopologyStoreId.SynchronizerStore]
            )
            .map(store => store.storeId.psid -> store)
            .toMap
          val allKnownLogical = synchronizerStores.keySet.map(_.logical)
          val allKnownActivePhysical =
            allKnownLogical.flatMap(physicalSynchronizerIdLookup.activePsidFor)
          val activePhysicalStores = allKnownActivePhysical.flatMap(synchronizerStores.get)
          activePhysicalStores.toSeq match {
            case Seq(synchronizerStore) => synchronizerStore.asRight
            case Seq() =>
              TopologyManagerError.TopologyStoreUnknown.NoSynchronizerStoreAvailable().asLeft
            case multiple =>
              TopologyManagerError.InvalidSynchronizer
                .MultipleSynchronizerStoresFound(multiple.map(_.storeId))
                .asLeft
          }
      }

    EitherT.fromEither[FutureUnlessShutdown](synchronizerStores)

  }

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult =
    new adminProto.BaseResult(
      store = Some(context.store.toProtoV30),
      sequenced = Some(context.sequenced.value.toProtoTimestamp),
      validFrom = Some(context.validFrom.value.toProtoTimestamp),
      validUntil = context.validUntil.map(_.value.toProtoTimestamp),
      operation = context.operation.toProto,
      transactionHash = context.transactionHash,
      serial = context.serial.unwrap,
      signedByFingerprints = context.signedBy.map(_.unwrap).toSeq,
    )

  // to avoid race conditions, we want to use the approximateTimestamp of the topology client.
  // otherwise, we might read stuff from the database that isn't yet known to the node
  private def getApproximateTimestamp(
      storeId: topology.store.TopologyStoreId
  ): Option[CantonTimestamp] = storeId match {
    case topology.store.TopologyStoreId.SynchronizerStore(psid) =>
      topologyClientLookup(psid).map(_.approximateTimestamp)
    case topology.store.TopologyStoreId.TemporaryStore(_) |
        topology.store.TopologyStoreId.AuthorizedStore =>
      None
  }

  private def collectFromStoresByFilterString(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: TopologyMapping.Code,
      filterString: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Seq[
    (TransactionSearchResult, TopologyMapping)
  ]] = {
    val (idFilter, namespaceFilter) = UniqueIdentifier.splitFilter(filterString)
    collectFromStores(
      baseQueryProto,
      typ,
      idFilter = Some(idFilter),
      namespaceFilter = namespaceFilter,
    )
  }

  /** Collects mappings of specified type from stores specified in baseQueryProto satisfying the
    * filters specified in baseQueryProto as well as separately specified filter either by a
    * namespace prefix (Left) or by a uid prefix (Right) depending on which applies to the mapping
    * type.
    */
  private def collectFromStores(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: TopologyMapping.Code,
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Seq[
    (TransactionSearchResult, TopologyMapping)
  ]] = {

    def fromStore(
        baseQuery: BaseQuery,
        store: topology.store.TopologyStore[topology.store.TopologyStoreId],
    ): FutureUnlessShutdown[Seq[(TransactionSearchResult, TopologyMapping)]] = {
      val storeId = store.storeId

      store
        .inspect(
          proposals = baseQuery.proposals,
          timeQuery = baseQuery.timeQuery,
          asOfExclusiveO = getApproximateTimestamp(storeId),
          op = baseQuery.ops,
          types = Seq(typ),
          idFilter = idFilter,
          namespaceFilter = namespaceFilter,
        )
        .flatMap { col =>
          col.result
            .filter(
              baseQuery.filterSigningKey.isEmpty || _.transaction.signatures
                .exists(_.authorizingLongTermKey.unwrap.startsWith(baseQuery.filterSigningKey))
            )
            .parTraverse { tx =>
              val resultE = for {
                // Re-create the signed topology transaction if necessary
                signedTx <- baseQuery.protocolVersion
                  .map { protocolVersion =>
                    SignedTopologyTransaction
                      .asVersion(tx.transaction, protocolVersion)(crypto)
                      .leftMap[Throwable](err =>
                        new IllegalStateException(s"Failed to convert topology transaction: $err")
                      )
                  }
                  .getOrElse {
                    // Keep the original transaction in its existing protocol version if no desired protocol version is specified
                    EitherT.rightT[FutureUnlessShutdown, Throwable](tx.transaction)
                  }

                result = TransactionSearchResult(
                  TopologyStoreId.fromInternal(storeId),
                  tx.sequenced,
                  tx.validFrom,
                  tx.validUntil,
                  signedTx.operation,
                  signedTx.transaction.hash.hash.getCryptographicEvidence,
                  signedTx.transaction.serial,
                  signedTx.signatures.map(_.authorizingLongTermKey),
                )
              } yield (result, tx.transaction.transaction.mapping)

              EitherTUtil.toFutureUnlessShutdown(resultE)
            }
        }
    }

    for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(baseQueryProto))
      stores <- collectStores(baseQuery.store)
      results <- EitherT.right(stores.parTraverse { store =>
        fromStore(baseQuery, store)
      })
    } yield {
      val res = results.flatten
      if (baseQuery.filterSigningKey.nonEmpty)
        res.filter(x => x._1.signedBy.exists(_.unwrap.startsWith(baseQuery.filterSigningKey)))
      else res
    }
  }

  override def listNamespaceDelegation(
      request: adminProto.ListNamespaceDelegationRequest
  ): Future[adminProto.ListNamespaceDelegationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        NamespaceDelegation.code,
        idFilter = None,
        namespaceFilter = Some(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: NamespaceDelegation)
              if request.filterTargetKeyFingerprint.isEmpty || x.target.fingerprint.unwrap == request.filterTargetKeyFingerprint =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListNamespaceDelegationResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListNamespaceDelegationResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listDecentralizedNamespaceDefinition(
      request: adminProto.ListDecentralizedNamespaceDefinitionRequest
  ): Future[adminProto.ListDecentralizedNamespaceDefinitionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DecentralizedNamespaceDefinition.code,
        idFilter = None,
        namespaceFilter = Some(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect { case (result, x: DecentralizedNamespaceDefinition) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDecentralizedNamespaceDefinitionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDecentralizedNamespaceDefinitionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        OwnerToKeyMapping.code,
        request.filterKeyOwnerUid,
      )
    } yield {
      val results = res
        .collect {
          // topology store indexes by uid, so need to filter out the members of the wrong type
          case (result, x: OwnerToKeyMapping)
              if x.member.filterString.startsWith(request.filterKeyOwnerUid) &&
                (request.filterKeyOwnerType.isEmpty || request.filterKeyOwnerType == x.member.code.threeLetterId.unwrap) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListOwnerToKeyMappingResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }
      adminProto.ListOwnerToKeyMappingResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyToKeyMapping(
      request: ListPartyToKeyMappingRequest
  ): Future[ListPartyToKeyMappingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyToKeyMapping.code,
        request.filterParty,
      )
    } yield {
      val results = res
        .collect { case (result, x: PartyToKeyMapping) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPartyToKeyMappingResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }
      adminProto.ListPartyToKeyMappingResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSynchronizerTrustCertificate(
      request: adminProto.ListSynchronizerTrustCertificateRequest
  ): Future[adminProto.ListSynchronizerTrustCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SynchronizerTrustCertificate.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: SynchronizerTrustCertificate) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSynchronizerTrustCertificateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSynchronizerTrustCertificateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyHostingLimits(
      request: ListPartyHostingLimitsRequest
  ): Future[ListPartyHostingLimitsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyHostingLimits.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: PartyHostingLimits) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPartyHostingLimitsResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyHostingLimitsResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listParticipantSynchronizerPermission(
      request: adminProto.ListParticipantSynchronizerPermissionRequest
  ): Future[adminProto.ListParticipantSynchronizerPermissionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        ParticipantSynchronizerPermission.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: ParticipantSynchronizerPermission) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListParticipantSynchronizerPermissionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListParticipantSynchronizerPermissionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        VettedPackages.code,
        request.filterParticipant,
      )
    } yield {
      val results = res
        .collect { case (result, x: VettedPackages) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListVettedPackagesResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListVettedPackagesResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyToParticipant.code,
        request.filterParty,
      )
    } yield {
      def partyPredicate(x: PartyToParticipant) =
        x.partyId.toProtoPrimitive.startsWith(request.filterParty)

      def participantPredicate(x: PartyToParticipant) =
        request.filterParticipant.isEmpty || x.participantIds.exists(
          _.toProtoPrimitive.contains(request.filterParticipant)
        )

      val results = res
        .collect {
          case (result, x: PartyToParticipant) if partyPredicate(x) && participantPredicate(x) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListPartyToParticipantResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyToParticipantResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSynchronizerParametersState(
      request: adminProto.ListSynchronizerParametersStateRequest
  ): Future[adminProto.ListSynchronizerParametersStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SynchronizerParametersState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: SynchronizerParametersState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSynchronizerParametersStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.parameters.toProtoV30),
          )
        }

      adminProto.ListSynchronizerParametersStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listMediatorSynchronizerState(
      request: adminProto.ListMediatorSynchronizerStateRequest
  ): Future[adminProto.ListMediatorSynchronizerStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        MediatorSynchronizerState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: MediatorSynchronizerState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListMediatorSynchronizerStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListMediatorSynchronizerStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSequencerSynchronizerState(
      request: adminProto.ListSequencerSynchronizerStateRequest
  ): Future[adminProto.ListSequencerSynchronizerStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SequencerSynchronizerState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: SequencerSynchronizerState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSequencerSynchronizerStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSequencerSynchronizerStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResponse] = Future.successful(
    adminProto.ListAvailableStoresResponse(storeIds =
      stores.map(s => TopologyStoreId.fromInternal(s.storeId).toProtoV30)
    )
  )

  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErrUS(
        request.excludeMappings.traverse(TopologyMapping.Code.fromString)
      )
      types = TopologyMapping.Code.all.diff(excludeTopologyMappings)
      storedTopologyTransactions <- listAllStoredTopologyTransactions(
        baseQuery,
        types,
        request.filterNamespace,
      )
    } yield {
      adminProto.ListAllResponse(result = Some(storedTopologyTransactions.toProtoV30))
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def exportTopologySnapshot(
      request: ExportTopologySnapshotRequest,
      responseObserver: StreamObserver[ExportTopologySnapshotResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient[ExportTopologySnapshotResponse](
      (out: OutputStream) => getTopologySnapshot(request, out),
      responseObserver,
      byteString => ExportTopologySnapshotResponse(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getTopologySnapshot(
      request: ExportTopologySnapshotRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErrUS(
        request.excludeMappings.traverse(TopologyMapping.Code.fromString)
      )
      types = TopologyMapping.Code.all.diff(excludeTopologyMappings)
      storedTopologyTransactions <- listAllStoredTopologyTransactions(
        baseQuery,
        types,
        request.filterNamespace,
      )

    } yield {
      val protocolVersion = baseQuery.protocolVersion.getOrElse(ProtocolVersion.latest)
      storedTopologyTransactions.toByteString(protocolVersion).writeTo(out)
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def exportTopologySnapshotV2(
      request: ExportTopologySnapshotV2Request,
      responseObserver: StreamObserver[ExportTopologySnapshotV2Response],
  ): Unit =
    GrpcStreamingUtils.streamToClient[ExportTopologySnapshotV2Response](
      (out: OutputStream) => getTopologySnapshotV2(request, out),
      responseObserver,
      byteString => ExportTopologySnapshotV2Response(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getTopologySnapshotV2(
      request: ExportTopologySnapshotV2Request,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErrUS(
        request.excludeMappings.traverse(TopologyMapping.Code.fromString)
      )
      types = TopologyMapping.Code.all.diff(excludeTopologyMappings)
      storedTopologyTransactions <- listAllStoredTopologyTransactions(
        baseQuery,
        types,
        request.filterNamespace,
      )

      protocolVersion = baseQuery.protocolVersion.getOrElse(ProtocolVersion.latest)
      _ <- wrapErrUS(
        EitherT.fromEither[FutureUnlessShutdown](
          MonadUtil
            .sequentialTraverse(storedTopologyTransactions.result)(
              _.writeDelimitedTo(protocolVersion, out)
            )
            .leftMap(error =>
              ProtoDeserializationError
                .ValueConversionError("topology_snapshot", error): ProtoDeserializationError
            )
        )
      )
    } yield ()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def listAllStoredTopologyTransactions(
      baseQuery: BaseQuery,
      topologyMappings: Seq[TopologyMapping.Code],
      filterNamespace: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, GenericStoredTopologyTransactions] =
    for {
      stores <- collectStores(baseQuery.store)
      results <- EitherT.right(
        stores.parTraverse { store =>
          store
            .inspect(
              proposals = baseQuery.proposals,
              timeQuery = baseQuery.timeQuery,
              asOfExclusiveO = getApproximateTimestamp(store.storeId),
              op = baseQuery.ops,
              types = topologyMappings,
              idFilter = None,
              namespaceFilter = Some(filterNamespace),
            )
        }
      ): EitherT[FutureUnlessShutdown, RpcError, Seq[GenericStoredTopologyTransactions]]
    } yield {
      val res = StoredTopologyTransactions(
        results.foldMap(
          _.result.filter(
            baseQuery.filterSigningKey.isEmpty || _.transaction.signatures.exists(
              _.authorizingLongTermKey.unwrap.startsWith(baseQuery.filterSigningKey)
            )
          )
        )
      )

      if (logger.underlying.isDebugEnabled()) {
        logger.debug(s"All listed topology transactions: ${res.result}")
      }
      res
    }

  override def genesisState(
      request: GenesisStateRequest,
      responseObserver: StreamObserver[GenesisStateResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => getGenesisState(request.synchronizerStore, request.timestamp, out),
      responseObserver,
      byteString => GenesisStateResponse(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getGenesisState(
      filterSynchronizerStore: Option[StoreId],
      timestamp: Option[Timestamp],
      out: OutputStream,
  ): Future[Unit] =
    for {
      (protocolVersion, source) <- getGenesisStateSource(filterSynchronizerStore, timestamp)
      storedTransactions <- source.runWith(Sink.seq)
    } yield StoredTopologyTransactions(storedTransactions)
      .toByteString(protocolVersion)
      .writeTo(out)

  override def genesisStateV2(
      request: GenesisStateV2Request,
      responseObserver: StreamObserver[GenesisStateV2Response],
  ): Unit = GrpcStreamingUtils.streamToClient(
    (out: OutputStream) => getGenesisStateV2(request.synchronizerStore, request.timestamp, out),
    responseObserver,
    byteString => GenesisStateV2Response(byteString),
    processingTimeout.unbounded.duration,
  )

  private def getGenesisStateV2(
      filterSynchronizerStore: Option[StoreId],
      timestamp: Option[Timestamp],
      out: OutputStream,
  ): Future[Unit] =
    for {
      (protocolVersion, source) <- getGenesisStateSource(filterSynchronizerStore, timestamp)
      _ <- source.runWith(
        Sink.foreachAsync(1) { stored =>
          val result = stored.writeDelimitedTo(protocolVersion, out)
          Future.fromTry(result.leftMap(new IllegalStateException(_)).toTry)
        }
      )
    } yield ()

  private def getGenesisStateSource(
      synchronizerStore: Option[StoreId],
      timestamp: Option[Timestamp],
  ): Future[(ProtocolVersion, Source[GenericStoredTopologyTransaction, NotUsed])] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val sourceEUS = for {
      _ <- member match {
        case _: ParticipantId =>
          wrapErrUS(
            ProtoConverter.required("synchronizer_store", synchronizerStore)
          )

        case _ => EitherTUtil.unitUS
      }
      topologyStoreO <- wrapErrUS(
        synchronizerStore.traverse(grpc.TopologyStoreId.fromProtoV30(_, "synchronizer_store"))
      )
      synchronizerTopologyStore <- collectSynchronizerStore(topologyStoreO)
      timestampO <- wrapErrUS(
        timestamp.traverse(CantonTimestamp.fromProtoTimestamp)
      )

      sequencedTimestamp <- timestampO match {
        case Some(value) => EitherT.rightT[FutureUnlessShutdown, RpcError](value)
        case None =>
          val sequencedTimeF = synchronizerTopologyStore
            .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
            .map {
              case Some((sequencedTime, _)) => Right(sequencedTime.value)

              case None =>
                Left(TopologyManagerError.TopologyTransactionNotFound.EmptyStore(): RpcError)
            }

          EitherT(sequencedTimeF)
      }

    } yield synchronizerTopologyStore.protocolVersion -> synchronizerTopologyStore
      .findEssentialStateAtSequencedTime(
        SequencedTime(sequencedTimestamp),
        includeRejected = false,
      )
      .map(stored =>
        StoredTopologyTransaction(
          SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
          EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime),
          stored.validUntil
            .map(_ => EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime)),
          stored.transaction,
          stored.rejectionReason,
        )
      )

    mapErrNewEUS(sourceEUS)
  }

  override def sequencerLsuState(
      request: SequencerLsuStateRequest,
      responseObserver: StreamObserver[SequencerLsuStateResponse],
  ): Unit = GrpcStreamingUtils.streamToClient(
    (out: OutputStream) =>
      getLogicalUpgradeState(request.synchronizerStore, request.timestamp, out),
    responseObserver,
    byteString => SequencerLsuStateResponse(byteString),
    processingTimeout.unbounded.duration,
  )

  private def getLogicalUpgradeState(
      synchronizerStore: Option[StoreId],
      tsOverride: Option[Timestamp],
      out: OutputStream,
  ): Future[Unit] =
    for {
      (protocolVersion, source) <- getLogicalUpgradeStateSource(synchronizerStore, tsOverride)
      _ <- source.runWith(
        Sink.foreachAsync(1) { stored =>
          val result = stored.writeDelimitedTo(protocolVersion, out)
          Future.fromTry(result.leftMap(new IllegalStateException(_)).toTry)
        }
      )
    } yield ()

  /** Gets the effective time of the LSU announcement
    */
  private def getLsuAnnouncementEffectiveTime(
      synchronizerTopologyStore: TopologyStore[
        com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
      ],
      topologyClient: SynchronizerTopologyClient,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, RpcError, EffectiveTime] = {
    val psid = synchronizerTopologyStore.storeId.psid

    for {
      // Find announcements in the store in the head state
      announcements <- EitherT.right(
        synchronizerTopologyStore
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(LsuAnnouncement.code),
            filterUid = Some(NonEmpty(Seq, psid.uid)),
            filterNamespace = None,
            pagination = None,
          )
          .map(_.collectOfMapping[LsuAnnouncement].result)
      )

      // Extract the effective time of the single effective announcement or raise an error accordingly.
      referenceEffectiveTime <- (announcements match {
        case Seq(single) =>
          EitherT.rightT[FutureUnlessShutdown, RpcError](single.validFrom)
        case Seq() =>
          EitherT.leftT[FutureUnlessShutdown, EffectiveTime][RpcError](
            TopologyManagerError.NoLsuAnnounced.Failure()
          )
        case multiple =>
          EitherT.leftT[FutureUnlessShutdown, EffectiveTime][RpcError](
            TopologyManagerError.InternalError.Unexpected(
              s"Found multiple LsuAnnouncement mappings, but only expected one: $multiple"
            )
          )
      })

      // Check for an announced LSU with the topology snapshot logic at the reference time.
      // This check is somewhat redundant, with the lookup above, however:
      // the topology snapshot likely contains additional validation logic that we don't want to copy
      // here but rather make use of.
      topologySnapshot <- EitherT.liftF(
        // Use the immediateSuccessor of the reference effective time, as that is the first
        // timestamp at which the transaction is effective in the topology state.
        topologyClient.awaitSnapshot(referenceEffectiveTime.immediateSuccessor.value)
      )
      _ <- EitherT.fromOptionF(
        fopt = topologySnapshot.announcedLsu(),
        ifNone = TopologyManagerError.NoLsuAnnounced.Failure(): RpcError,
      )
    } yield referenceEffectiveTime
  }

  private def getLogicalUpgradeStateSource(
      synchronizerStore: Option[StoreId],
      tsOverrideP: Option[Timestamp],
  ): Future[(ProtocolVersion, Source[GenericStoredTopologyTransaction, NotUsed])] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val sourceEUS = for {
      _ <- member match {
        case _: ParticipantId =>
          wrapErrUS(ProtoConverter.required("synchronizer_store", synchronizerStore))

        case _ => EitherT.rightT[FutureUnlessShutdown, RpcError](())
      }
      topologyStoreO <- wrapErrUS(
        synchronizerStore.traverse(
          grpc.TopologyStoreId.fromProtoV30(_, "synchronizer_store")
        )
      )

      synchronizerTopologyStore <- collectSynchronizerStore(topologyStoreO)
      psid = synchronizerTopologyStore.storeId.psid

      timeTracker <- EitherT.fromOption[FutureUnlessShutdown](
        timeTrackerLookup(psid),
        TopologyManagerError.InternalError.Unexpected(
          s"Unable to find synchronizer time tracker for $psid."
        ),
      )

      topologyClient <- EitherT.fromEither[FutureUnlessShutdown](
        topologyClientLookup(psid).toRight(
          TopologyManagerError.TopologyStoreUnknown.Failure(synchronizerTopologyStore.storeId)
        )
      )

      // Extract the effective time of the single effective announcement or raise an error accordingly.
      tsOverrideO <- wrapErrUS(tsOverrideP.traverse(CantonTimestamp.fromProtoTimestamp))
      referenceEffectiveTime <- tsOverrideO match {
        case Some(tsOverride) =>
          EitherT.pure[FutureUnlessShutdown, RpcError](EffectiveTime(tsOverride))
        case None => getLsuAnnouncementEffectiveTime(synchronizerTopologyStore, topologyClient)
      }

      _ = logger.info(s"Computing sequencer LSU state at $referenceEffectiveTime")

      // Wait for effective time to be observed on the synchronizer (and optionally request a time tick).
      // We need to wait for all transactions with a sequencedTime <= referenceEffectiveTime, otherwise we would
      // miss them in the topology snapshot.
      _ <- EitherT.right[RpcError](
        FutureUnlessShutdown.outcomeF(
          timeTracker.awaitTick(referenceEffectiveTime.value).getOrElse(Future.unit)
        )
      )

      // Wait for the topology client to have observed the effective time as sequenced time.
      // We need to wait for the topology client to observe referenceEffectiveTime as sequenced time,
      // so that we know all the topology processing up to that timestamp has completed.
      _ <- EitherT.right[RpcError](
        topologyClient
          .awaitSequencedTimestamp(SequencedTime(referenceEffectiveTime.value))
          .getOrElse(FutureUnlessShutdown.unit)
      )
    } yield {
      // Now all the stores are in sync and we can actually query the store

      // The specific filter here must be kept in sync with the filters for the local copy in
      // - DbTopologyStore.copyFromPredecessorSynchronizerStore
      // - InMemoryTopologyStore.copyFromPredecessorSynchronizerStore
      synchronizerTopologyStore.protocolVersion -> synchronizerTopologyStore
        .findEssentialStateAtSequencedTime(
          SequencedTime(referenceEffectiveTime.value),
          includeRejected = false,
        )
        .filter { stored =>
          val isNonLsu =
            !TopologyMapping.Code.lsuMappingsExcludedFromUpgrade.contains(stored.mapping.code)
          val isFullyAuthorizedOrNotExpiredProposal =
            !stored.transaction.isProposal || stored.validUntil.isEmpty

          isNonLsu && isFullyAuthorizedOrNotExpiredProposal
        }
    }
    CantonGrpcUtil.mapErrNewEUS(sourceEUS)
  }

  override def listLsuAnnouncement(
      request: ListLsuAnnouncementRequest
  ): Future[ListLsuAnnouncementResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        LsuAnnouncement.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res.collect { case (context, announcement: LsuAnnouncement) =>
        adminProto.ListLsuAnnouncementResponse.Result(
          context = Some(createBaseResult(context)),
          item = Some(announcement.toProto),
        )
      }
      adminProto.ListLsuAnnouncementResponse(results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listLsuSequencerConnectionSuccessor(
      request: ListLsuSequencerConnectionSuccessorRequest
  ): Future[ListLsuSequencerConnectionSuccessorResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        LsuSequencerConnectionSuccessor.code,
        request.filterSequencerId,
      )
    } yield {
      val filterSuccessorPhysicalSynchronizerId =
        OptionUtil.emptyStringAsNone(request.filterSuccessorPhysicalSynchronizerId)
      def successorPsidPredicate(mapping: LsuSequencerConnectionSuccessor) =
        filterSuccessorPhysicalSynchronizerId.fold(true)(
          _.startsWith(mapping.successorPsid.toProtoPrimitive)
        )

      val results = res.collect {
        case (context, successor: LsuSequencerConnectionSuccessor)
            if (successorPsidPredicate(successor)) =>
          adminProto.ListLsuSequencerConnectionSuccessorResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(successor.toProto),
          )
      }
      adminProto.ListLsuSequencerConnectionSuccessorResponse(results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
