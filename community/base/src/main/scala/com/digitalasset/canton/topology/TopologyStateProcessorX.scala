// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class TopologyStateProcessorX(
    val store: TopologyStoreX[TopologyStoreId],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  // small container to store potentially pending data
  private case class MaybePending(originalTx: GenericSignedTopologyTransactionX) {
    val adjusted = new AtomicReference[Option[GenericSignedTopologyTransactionX]](None)
    val rejection = new AtomicReference[Option[TopologyTransactionRejection]](None)

    def currentTx: GenericSignedTopologyTransactionX = adjusted.get().getOrElse(originalTx)

    def validatedTx: GenericValidatedTopologyTransactionX =
      ValidatedTopologyTransactionX(currentTx, rejection.get())
  }

  // TODO(#11255) use cache instead and remember empty
  private val txForMapping = TrieMap[MappingHash, MaybePending]()
  private val proposalsByMapping = TrieMap[MappingHash, Seq[TxHash]]()
  private val proposalsForTx = TrieMap[TxHash, MaybePending]()

  // compared to the old topology stores, the x stores don't distinguish between
  // state & transaction store. cascading deletes are irrevocable and delete everything
  // that depended on a certificate.

  /** validate the authorization and the signatures of the given transactions
    *
    * The function is NOT THREAD SAFE AND MUST RUN SEQUENTIALLY
    */
  def validateAndApplyAuthorization(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransactionX],
      // TODO(#11255) propagate and abort unless we use force
      abortIfCascading: Boolean,
      abortOnError: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[GenericValidatedTopologyTransactionX], Unit] = {

    type Lft = Seq[GenericValidatedTopologyTransactionX]

    // first, pre-load the currently existing mappings and proposals for the given transactions
    val preloadTxsForMappingF = preloadTxsForMapping(effective, transactions)
    val preloadProposalsForTxF = preloadProposalsForTx(effective, transactions)
    // TODO(#11255) preload authorization data
    val ret = for {
      _ <- EitherT.right[Lft](preloadProposalsForTxF)
      _ <- EitherT.right[Lft](preloadTxsForMappingF)
      // compute / collapse updates
      ((mappingRemoves, txRemoves), pendingWrites) = {
        val pendingWrites = transactions.map(MaybePending)
        val removes = pendingWrites
          .foldLeft((Set.empty[MappingHash], Set.empty[TxHash])) {
            case ((removeMappings, removeTxs), tx) =>
              val finalTx = validateAndMerge(tx.originalTx, expectFullAuthorization)
              tx.adjusted.set(Some(finalTx.transaction))
              tx.rejection.set(finalTx.rejectionReason)
              determineRemovesAndUpdatePending(tx, removeMappings, removeTxs)
          }
        (removes, pendingWrites)
      }
      validatedTx = pendingWrites.map(_.validatedTx)
      _ <- EitherT.cond[Future](
        // TODO(#11255) differentiate error reason and only abort actual errors, not in-batch merges
        !abortOnError || validatedTx.forall(_.rejectionReason.isEmpty),
        (), {
          // reset caches as they are broken now if we abort
          txForMapping.clear()
          proposalsForTx.clear()
          // TODO(#11255) reset authorization state too
          validatedTx
        }: Lft,
      ): EitherT[Future, Lft, Unit]
      _ <- EitherT.right[Lft](
        store.update(
          sequenced,
          effective,
          mappingRemoves,
          txRemoves,
          validatedTx,
        )
      )
    } yield validatedTx
    ret.bimap(
      failed => {
        logger.info("Topology transactions failed:\n  " + failed.mkString("\n  "))
        failed
      },
      success => {
        logger.info(
          "Persisted topology transactions:\n  " + success.mkString("\n  ")
        )
        ()
      },
    )
  }

  private def preloadTxsForMapping(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes = transactions
      .map(x => x.transaction.mapping.uniqueKey)
      .filterNot(txForMapping.contains)
    if (hashes.nonEmpty) {
      store
        .findTransactionsForMapping(effective, hashes)
        .map(_.foreach { item =>
          txForMapping.put(item.transaction.mapping.uniqueKey, MaybePending(item)).discard
        })
    } else Future.unit
  }

  private def trackProposal(txHash: TxHash, mappingHash: MappingHash): Unit = {
    proposalsByMapping
      .updateWith(mappingHash) {
        case None => Some(Seq(txHash))
        case Some(seq) => Some(seq :+ txHash)
      }
      .discard
  }

  private def preloadProposalsForTx(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes =
      transactions.map(x => x.transaction.hash).filterNot(proposalsForTx.contains)
    if (hashes.nonEmpty) {
      store
        .findProposalsByTxHash(effective, hashes)
        .map(_.foreach { item =>
          val txHash = item.transaction.hash
          // store the proposal
          proposalsForTx.put(txHash, MaybePending(item)).discard
          // maintain a map from mapping to txs
          trackProposal(txHash, item.transaction.mapping.uniqueKey)
        })
    } else Future.unit
  }

  private def serialIsMonotonicallyIncreasing(
      previous: Option[GenericSignedTopologyTransactionX],
      current: GenericSignedTopologyTransactionX,
  ): Either[String, Unit] = previous match {
    case Some(value) =>
      Either.cond(
        value.transaction.serial.value + 1 == current.transaction.serial.value,
        (),
        s"Serial mismatch: active=${value.transaction.serial}, update=${value.transaction.serial}",
      )
    case None => Right(())
  }

  private def transactionIsAuthorized(
      previous: Option[GenericSignedTopologyTransactionX],
      current: GenericSignedTopologyTransactionX,
      expectFullAuthorization: Boolean,
  ): Either[String, GenericSignedTopologyTransactionX] = {
    // if it is a proposal, then each signature must be valid, but we don't need
    // the signatures to be sufficient
    // if the tx is a proposal but has enough signatures, emit log message
    // that it is now fully authorized and counts as state now
    // TODO(#11255) @gerolf: have fun
    //  - check that signatures are correct
    //  - check whether the keys are allowed to sign the tx
    //  - check if there are enough signatures
    Right(
      SignedTopologyTransactionX(
        current.transaction,
        current.signatures,
        isProposal = false, // TODO(#11255) only set to false if it is authorized
      )(current.representativeProtocolVersion)
    )
  }

  def potentiallyMergeWithLast(
      previous: Option[GenericSignedTopologyTransactionX],
      current: GenericSignedTopologyTransactionX,
  ): (Boolean, GenericSignedTopologyTransactionX) = previous match {
    case Some(value) if value.transaction.hash == current.transaction.hash =>
      // TODO(#11255) deduplicate if no new signatures are being added
      // => DuplicateTransaction -> reject or ignore
      (true, value.addSignatures(current.signatures.toSeq))
    case _ => (false, current)
  }

  private def fetchPendingProposalAndMerge(
      current: GenericSignedTopologyTransactionX
  ): GenericSignedTopologyTransactionX = {
    proposalsForTx.get(current.transaction.hash) match {
      case None => current
      case Some(previous) =>
        SignedTopologyTransactionX(
          current.transaction,
          signatures = current.signatures ++ previous.currentTx.signatures,
          isProposal = current.isProposal,
        )(current.representativeProtocolVersion)
    }
  }

  private def validateAndMerge(
      txA: GenericSignedTopologyTransactionX,
      expectFullAuthorization: Boolean,
  ): GenericValidatedTopologyTransactionX = {
    // get current valid transaction for the given mapping
    val last = txForMapping.get(txA.transaction.mapping.uniqueKey).map(_.currentTx)
    // first, merge a pending proposal with this transaction. we do this as it might
    // subsequently activate the given transaction
    val txB = fetchPendingProposalAndMerge(txA)
    // TODO(#11255) add a check here for consistency. these checks should reject on the mediator / topology
    //   manager, but only warn on the processor.
    //   things we need to catch are:
    //     - a party to participant mapping mentioning a participant who is not on the domain
    //       (no trust certificate or no owner keys)
    //     - a mediator or sequencer who doesn't have keys
    val ret = for {
      // we check if the transaction is properly authorized given the current topology state
      // if it is a proposal, then we demand that all signatures are appropriate (but
      // not necessarily sufficient)
      txC <- transactionIsAuthorized(last, txB, expectFullAuthorization)
      // we potentially merge the transaction with the currently active if this is just a signature update
      (isMerge, txD) = potentiallyMergeWithLast(last, txC)
      // now, check if the serial is monotonically increasing
      _ <-
        if (isMerge)
          Right(())
        else serialIsMonotonicallyIncreasing(last, txD)
    } yield txD
    ret match {
      case Right(value) => ValidatedTopologyTransactionX(value, None)
      case Left(value) =>
        // TODO(#11255) emit appropriate log message and use correct rejection reason
        ValidatedTopologyTransactionX(txA, Some(TopologyTransactionRejection.Other(value)))
    }
  }

  private def determineRemovesAndUpdatePending(
      tx: MaybePending,
      removeMappings: Set[MappingHash],
      removeTxs: Set[TxHash],
  )(implicit traceContext: TraceContext): (Set[MappingHash], Set[TxHash]) = {
    val finalTx = tx.currentTx
    // UPDATE tx SET valid_until = effective WHERE storeId = XYZ
    //    AND valid_until is NULL and valid_from < effective
    // if this is a proposal, we only delete the "previously existing proposal"
    if (finalTx.isProposal) {
      // AND ((tx_hash = ..))
      val txHash = finalTx.transaction.hash
      proposalsForTx.put(txHash, tx).foreach { previous =>
        // update currently pending (this is relevant in case if we have within a batch proposals for the
        // same txs)
        val cur = previous.rejection
          .getAndSet(Some(TopologyTransactionRejection.Other("Merged with other proposal")))
        ErrorUtil.requireState(cur.isEmpty, s"Error state should be empty for ${previous}")
      }
      trackProposal(txHash, finalTx.transaction.mapping.uniqueKey)
      (removeMappings, removeTxs + txHash)
    } else {
      // if this is a sufficiently signed and valid transaction, we delete all existing proposals and the previous tx
      //  we can just use a mapping key: there can not be a future proposal, as it would violate the
      //  monotonically increasing check
      // AND ((mapping_key = ...) )
      val mappingHash = finalTx.transaction.mapping.uniqueKey
      txForMapping.put(mappingHash, tx).foreach { previous =>
        // replace previous tx in case we have concurrent updates within the same timestamp
        val cur = previous.rejection
          .getAndSet(Some(TopologyTransactionRejection.Other("Subsumed within batch")))
        ErrorUtil.requireState(cur.isEmpty, s"Error state should be empty for ${previous}")
      }
      // remove all pending proposals for this mapping
      proposalsByMapping
        .remove(mappingHash)
        .foreach(
          _.foreach(proposal =>
            proposalsForTx.remove(proposal).foreach { existing =>
              val cur = existing.rejection.getAndSet(
                Some(TopologyTransactionRejection.Other("Outdated proposal within batch"))
              )
              ErrorUtil.requireState(cur.isEmpty, s"Error state should be empty for ${existing}")
            }
          )
        )
      // TODO(#11255) if this is a removal of a certificate, compute cascading deletes
      //   if boolean flag is set, then abort, otherwise notify
      //   rules: if a namespace delegation is a root delegation, it won't be affected by the
      //          cascading deletion of its authorizer. this will allow us to roll namespace certs
      //          also, root cert authorization is only valid if serial == 1
      (removeMappings + mappingHash, removeTxs)
    }
  }
}
