// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.topology.admin.grpc.GrpcTopologyAggregationService.MemberKeyRecord
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.store.{
  NoPackageDependencies,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  Member,
  MemberCode,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp

import scala.concurrent.{ExecutionContext, Future}

/** Aggregates topology info across synchronizer stores.
  *
  * Concurrency Design: Uses strict **sequential traversal** over synchronizers. Nodes typically
  * connect to only a few synchronizers (!), making sequential latency negligible.
  */
class GrpcTopologyAggregationService(
    stores: => Seq[TopologyStore[TopologyStoreId.SynchronizerStore]],
    ips: IdentityProvidingServiceClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyAggregationServiceGrpc.TopologyAggregationService
    with NamedLogging {

  private def getTopologySnapshot(
      asOf: CantonTimestamp,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
  ): TopologySnapshotLoader =
    new StoreBasedTopologySnapshot(
      store.storeId.psid,
      asOf,
      store,
      NoPackageDependencies,
      loggerFactory,
    )

  private def snapshots(
      synchronizerIds: Set[SynchronizerId],
      asOf: Option[ProtoTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, List[
    (PhysicalSynchronizerId, TopologySnapshotLoader)
  ]] =
    wrapErrUS(asOf.traverse(CantonTimestamp.fromProtoTimestamp)).map { asOfO =>
      stores.collect {
        case store
            if synchronizerIds.contains(store.storeId.psid.logical) || synchronizerIds.isEmpty =>
          val synchronizerId = store.storeId.psid
          // Get the approximate timestamp from the synchronizer client to prevent race conditions
          // (when we have written data into the stores but haven't yet updated the client)
          val effectiveAsOf = asOfO.getOrElse(
            ips
              .forSynchronizer(synchronizerId)
              .map(_.approximateTimestamp)
              .getOrElse(CantonTimestamp.MaxValue)
          )
          (
            synchronizerId,
            getTopologySnapshot(effectiveAsOf, store),
          )
      }.toList
    }

  /** Sequentially finds parties matching filters, short-circuiting at `limit`.
    *
    * Scaling impact: High limits risk heavy JVM memory pressure as the accumulated `Set[PartyId]`
    * is held entirely in memory.
    */
  private def findMatchingParties(
      clients: List[(PhysicalSynchronizerId, TopologySnapshotLoader)],
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = MonadUtil
    .foldLeftM((Set.empty[PartyId], false), clients) { case ((res, isDone), (_, client)) =>
      if (isDone) FutureUnlessShutdown.pure((res, true))
      else
        client.inspectKnownParties(filterParty, filterParticipant, limit = limit).map { found =>
          val tmp = found ++ res
          if (tmp.sizeIs >= limit) (tmp.take(limit), true) else (tmp, false)
        }
    }
    .map { case (res, _) => res }

  /** Lists parties and their participants across synchronizers.
    *
    * Scaling impact: An excessively high `limit` pushes massive arrays to the DB, risking DB-level
    * constraints (e.g., query size bounds) and JVM OutOfMemory errors during response mapping.
    */
  override def listParties(
      request: v30.ListPartiesRequest
  ): Future[v30.ListPartiesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ListPartiesRequest(asOfP, limit, synchronizerIdsP, filterParty, filterParticipant) =
      request

    val res: EitherT[FutureUnlessShutdown, RpcError, v30.ListPartiesResponse] = for {
      synchronizerIds <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerIdsP.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_ids"))
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)

      matched <- snapshots(synchronizerIds.toSet, asOfP)

      parties <- EitherT.right(
        findMatchingParties(matched, filterParty, filterParticipant, limit)
      )

      partyIdLookup = parties.map(p => p.toLf -> p).toMap
      lfParties = partyIdLookup.keys.toSeq

      // Execute exactly ONE bulk query per synchronizer, sequentially.
      results <- EitherT.right(
        MonadUtil.foldLeftM(
          Map
            .empty[PartyId, Map[ParticipantId, Map[PhysicalSynchronizerId, ParticipantPermission]]],
          matched,
        ) { case (acc, (synchronizerId, client)) =>
          client.activeParticipantsOfPartiesWithInfo(lfParties).map { partiesInfoMap =>
            partiesInfoMap.foldLeft(acc) { case (partyAcc, (lfParty, partyInfo)) =>
              val partyId = partyIdLookup(lfParty)
              val currentPartyParticipants = partyAcc.getOrElse(partyId, Map.empty)

              val updatedPartyParticipants =
                partyInfo.participants.foldLeft(currentPartyParticipants) {
                  case (participantAcc, (participantId, attributes)) =>
                    val existingSyncs = participantAcc.getOrElse(participantId, Map.empty)
                    participantAcc.updated(
                      participantId,
                      existingSyncs + (synchronizerId -> attributes.permission),
                    )
                }

              partyAcc.updated(partyId, updatedPartyParticipants)
            }
          }
        }
      )
    } yield {
      v30.ListPartiesResponse(
        results = results.view.map { case (partyId, participants) =>
          v30.ListPartiesResponse.Result(
            party = partyId.toProtoPrimitive,
            participants = participants.map { case (participantId, synchronizers) =>
              v30.ListPartiesResponse.Result.ParticipantSynchronizers(
                participantUid = participantId.uid.toProtoPrimitive,
                synchronizers = synchronizers.map { case (synchronizerId, permission) =>
                  v30.ListPartiesResponse.Result.ParticipantSynchronizers.SynchronizerPermissions(
                    synchronizerId = synchronizerId.logical.toProtoPrimitive,
                    permission = permission.toProtoV30,
                    physicalSynchronizerId = synchronizerId.toProtoPrimitive,
                  )
                }.toSeq,
              )
            }.toSeq,
          )
        }.toSeq
      )
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  /** Lists key owners and their signing/encryption keys across specified synchronizers.
    *
    * Scaling impact: `request.limit` restrict keys *per owner*, not total owners. Broad queries on
    * large networks will yield massive result sets, risking JVM OOM during grouping.
    */
  override def listKeyOwners(
      request: v30.ListKeyOwnersRequest
  ): Future[v30.ListKeyOwnersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: EitherT[FutureUnlessShutdown, RpcError, v30.ListKeyOwnersResponse] = for {
      keyOwnerTypeO <- wrapErrUS(
        OptionUtil
          .emptyStringAsNone(request.filterKeyOwnerType)
          .traverse(code => MemberCode.fromProtoPrimitive(code, "filterKeyOwnerType"))
      ): EitherT[FutureUnlessShutdown, RpcError, Option[MemberCode]]

      synchronizerIds <- EitherT
        .fromEither[FutureUnlessShutdown](
          request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_ids"))
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)

      matched <- snapshots(synchronizerIds.toSet, request.asOf)

      // We iterate over the synchronizers sequentially rather than in parallel.
      // Sequential traversal ensures we only enqueue one DB task at a time per request,
      // preventing API traffic spikes from starving other critical node operations that
      // share the same ExecutionContext.
      res <- EitherT.right(MonadUtil.sequentialTraverse(matched) { case (storeId, client) =>
        client.inspectKeys(request.filterKeyOwnerUid, keyOwnerTypeO, request.limit).map { res =>
          (storeId, res)
        }
      })
    } yield {
      val records = for {
        (storeId, keyPerMember) <- res
        (owner, keys) <- keyPerMember
      } yield MemberKeyRecord(owner, storeId, keys)

      val mapped = records.groupMap(r => r.owner)(r => (r.storeId, r.keys))

      v30.ListKeyOwnersResponse(
        results = mapped.toSeq.flatMap { case (owner, keyPerSynchronizer) =>
          keyPerSynchronizer.map { case (psid, keys) =>
            v30.ListKeyOwnersResponse.Result(
              keyOwner = owner.toProtoPrimitive,
              synchronizerId = psid.logical.toProtoPrimitive,
              signingKeys = keys.signingKeys.map(_.toProtoV30),
              encryptionKeys = keys.encryptionKeys.map(_.toProtoV30),
              physicalSynchronizerId = psid.toProtoPrimitive,
            )
          }
        }
      )
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }
}

object GrpcTopologyAggregationService {

  private final case class MemberKeyRecord[K](
      owner: Member,
      storeId: PhysicalSynchronizerId,
      keys: K,
  )

}
