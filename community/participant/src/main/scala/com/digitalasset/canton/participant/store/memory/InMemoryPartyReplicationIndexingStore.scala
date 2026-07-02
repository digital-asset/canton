// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ActivationChangeBatchEntry,
  ContractActivationChangeBatch,
  Watermark,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{ReassignmentCounter, checked}
import com.digitalasset.nonempty.NonEmpty

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class InMemoryPartyReplicationIndexingStore(
    override protected val pauseIndexingDuringOnPR: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends PartyReplicationIndexingStore
    with NamedLogging {

  private val state = new AtomicReference[InMemoryPartyReplicationIndexingStore.State](
    InMemoryPartyReplicationIndexingStore.State(
      partyReplicationIndexing = new TreeSet[ActivationChangeBatchEntry](),
      indexingWatermark = Watermark.MinValue,
      indexedWatermark = Watermark.MinValue,
    )
  )

  override def addImportedContractActivations(
      watermark: Watermark,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    state.updateAndGet {
      case InMemoryPartyReplicationIndexingStore.State(
            activationChanges,
            indexingWatermark,
            indexedWatermark,
          ) =>
        InMemoryPartyReplicationIndexingStore.State(
          activationChanges ++ contractActivationsToIndex.zipWithIndex.map {
            case (ContractReassignment(contract, _, _, ctr), index) =>
              ActivationChangeBatchEntry(
                Watermark(
                  watermark.timestamp,
                  watermark.counter + checked(NonNegativeLong.tryCreate(index.toLong)),
                ),
                contract.contractId,
                ChangeType.Activation,
                ctr,
              )
          },
          indexingWatermark,
          indexedWatermark,
        )
    }.discard
    FutureUnlessShutdown.unit
  }

  override def addContractActivationChanges(
      timestamp: CantonTimestamp,
      changes: Seq[(LfContractId, (ChangeType, ReassignmentCounter))],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (!pauseIndexingDuringOnPR) {
      state.updateAndGet {
        case InMemoryPartyReplicationIndexingStore.State(
              activationChanges,
              indexingWatermark,
              indexedWatermark,
            ) =>
          InMemoryPartyReplicationIndexingStore.State(
            activationChanges ++ changes.zipWithIndex.map {
              case ((contractId, (change, ctr)), index) =>
                ActivationChangeBatchEntry(
                  Watermark(timestamp, checked(NonNegativeLong.tryCreate(index.toLong))),
                  contractId,
                  change,
                  ctr,
                )
            },
            indexingWatermark,
            indexedWatermark,
          )
      }.discard
    }
    FutureUnlessShutdown.unit
  }

  override def consumeNextActivationChangesBatch(maxBatchSize: PositiveInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ContractActivationChangeBatch]] = {

    // Get initial indexing watermark (outside updateAndGet) which is allowed because
    // the `PartyReplicationIndexingStore.consumeNextActivationChangesBatch` method cannot be called
    // multiple times concurrently.
    val indexingWatermarkBefore = state.get().indexingWatermark
    val updatedState = state.updateAndGet {
      case previousState @ InMemoryPartyReplicationIndexingStore.State(
            activationChanges,
            indexingWatermark,
            indexedWatermark,
          ) =>
        NonEmpty
          .from(
            activationChanges.toSeq
              .collect {
                case entry: ActivationChangeBatchEntry if entry.watermark > indexingWatermark =>
                  entry
              }
              .take(maxBatchSize.unwrap)
          )
          .fold(previousState) { changesNE =>
            val trimmedChangesNE = trimActivationChangesBatch(changesNE)
            InMemoryPartyReplicationIndexingStore.State(
              activationChanges,
              trimmedChangesNE.last1.watermark,
              indexedWatermark,
            )
          }
    }
    FutureUnlessShutdown.pure(
      NonEmpty
        .from(
          updatedState.partyReplicationIndexing.toSeq
            .collect[(LfContractId, (ChangeType, ReassignmentCounter))] {
              case entry: ActivationChangeBatchEntry
                  if entry.watermark > indexingWatermarkBefore && entry.watermark <= updatedState.indexingWatermark =>
                entry.contractId -> (entry.change, entry.reassignmentCounter)
            }
        )
        .map(ContractActivationChangeBatch(_, updatedState.indexingWatermark))
    )
  }

  override def markContractActivationChangesAsIndexed(watermark: Watermark)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    state.updateAndGet(_.copy(indexedWatermark = watermark)).discard
    FutureUnlessShutdown.unit
  }

  override def purgeContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    state.updateAndGet { case InMemoryPartyReplicationIndexingStore.State(changes, _, _) =>
      InMemoryPartyReplicationIndexingStore.State(
        changes.empty,
        Watermark.MinValue,
        Watermark.MinValue,
      )
    }
    FutureUnlessShutdown.unit
  }

  override def deleteSince(deleteStartingAtInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    state.updateAndGet {
      case InMemoryPartyReplicationIndexingStore.State(
            changes,
            indexingWatermark,
            indexedWatermark,
          ) =>
        val preservedChanges = changes.collect {
          case entry: ActivationChangeBatchEntry
              if entry.watermark.timestamp < deleteStartingAtInclusive =>
            entry
        }
        val lastWatermarkO = preservedChanges.lastOption.map(_.watermark)

        // Lower any watermark to not fall into the deleted portion.
        def lowerWatermarkIfNecessary(watermark: Watermark): Watermark =
          if (lastWatermarkO.exists(_ >= watermark)) {
            watermark
          } else lastWatermarkO.getOrElse(Watermark.MinValue)

        InMemoryPartyReplicationIndexingStore.State(
          preservedChanges,
          lowerWatermarkIfNecessary(indexingWatermark),
          lowerWatermarkIfNecessary(indexedWatermark),
        )
    }
    FutureUnlessShutdown.unit
  }

  override protected[store] def listContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ActivationChange]] = {
    val snapshot = state.get()

    FutureUnlessShutdown.pure(snapshot.partyReplicationIndexing.toSeq.map {
      case ActivationChangeBatchEntry(watermark, contractId, changeType, reassignmentCounter) =>
        ActivationChange(
          watermark,
          contractId,
          changeType,
          watermark <= snapshot.indexingWatermark,
          watermark <= snapshot.indexedWatermark,
          reassignmentCounter,
        )
    })
  }
}

object InMemoryPartyReplicationIndexingStore {
  private final case class State(
      partyReplicationIndexing: TreeSet[ActivationChangeBatchEntry],
      indexingWatermark: Watermark,
      indexedWatermark: Watermark,
  )
}
