// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.checks.RequiredTopologyMappingChecks
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.*

/** Validates an initial topology snapshot by:
  *
  *   - running transaction authorization validation
  *   - deduplicating topology transactions
  *   - removing superfluous signatures
  *   - checking that serials are strictly monotonic
  *   - checking that rejected transactions in the initial snapshot also are rejected in stored
  *     snapshot
  *   - checking that the effective times valid_from and valid_until of the transactions are the
  *     same in the initial snapshot and the stored snapshot
  *
  * Also compares the computed effective time with the effective provided in the snapshot.
  *
  * Any inconsistency between the topology snapshot and the outcome of the validation is reported.
  */
class InitialTopologySnapshotValidator(
    pureCrypto: CryptoPureApi,
    store: TopologyStore[TopologyStoreId],
    batchingConfig: BatchingConfig,
    topologyConfig: TopologyConfig,
    staticSynchronizerParameters: Option[StaticSynchronizerParameters],
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, materializer: Materializer)
    extends NamedLogging {

  protected val stateProcessor: TopologyStateProcessor =
    TopologyStateProcessor.forInitialSnapshotValidation(
      store,
      batchingConfig.topologyCacheAggregator,
      topologyConfig,
      lookup =>
        new RequiredTopologyMappingChecks(
          staticSynchronizerParameters,
          store.protocolVersion,
          lookup,
          loggerFactory,
        ),
      pureCrypto,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  /** Runs the topology snapshot through the normal processing/validation pipeline of the
    * TopologyStateProcessor.
    *
    * '''NOTICE''':
    *   - the preparation and pre-processing of the provided topology snapshot is only done to
    *     support a wider variety of (legacy) topology snapshots.
    *   - The outcome of the validation and import is compared with the expected outcome of the
    *     snapshot. Any inconsistencies are raised as errors. This serves as a security barrier.
    */
  final def validateAndApplyInitialTopologySnapshot(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    validateAndApplyInitialTopologySnapshot(Source(initialSnapshot.result))

  final def validateAndApplyInitialTopologySnapshot(
      initialSnapshotSource: Source[GenericStoredTopologyTransaction, NotUsed]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val lastUpdate = new AtomicLong(System.nanoTime())

    def runGraph(
        graph: RunnableGraph[Future[Option[UnlessShutdown[Unit]]]]
    ): FutureUnlessShutdown[Unit] = {
      lastUpdate.set(System.nanoTime())

      FutureUnlessShutdown(
        PekkoUtil
          .runSupervised(
            graph,
            "initial-topology-bulk-insert",
            (doneF: Future[?]) => doneF.isCompleted,
          )
          .map {
            case Some(UnlessShutdown.AbortedDueToShutdown) => UnlessShutdown.AbortedDueToShutdown
            case _ => UnlessShutdown.Outcome(())
          }
      )
    }

    def emitProgress(lastUpdate: AtomicLong)(tx: GenericStoredTopologyTransaction)(implicit
        traceContext: TraceContext
    ): Unit = {
      val now = System.nanoTime()
      val last = lastUpdate.get()
      if (TimeUnit.NANOSECONDS.toSeconds(now - last) >= 15 && lastUpdate.compareAndSet(last, now)) {
        logger.info(
          s"Progressed topology download to sequenced=${tx.sequenced}, validFrom=${tx.validFrom.value})"
        )
        lastUpdate.set(now)
      }
    }

    val sourceWithCheckedTimestamps = initialSnapshotSource
      // ensure that the stream is ordered with monotonically increasing sequenced times, otherwise the
      // groupAdjacentBy yields unexpected batches of topology transactions
      .statefulMap(() => (SequencedTime.MinValue, EffectiveTime.MinValue))(
        { case ((prevTimestamp, prevValidFrom), current) =>
          if (current.sequenced < prevTimestamp)
            throw new IllegalArgumentException(
              s"Received non-monotonic sequenced times. previous=$prevTimestamp, current: ${current.sequenced} from event $current"
            )
          if (current.validFrom < prevValidFrom)
            throw new IllegalArgumentException(
              s"Received non-monotonic effective times. previous=$prevValidFrom, current: ${current.validFrom} from event $current"
            )
          if (prevValidFrom != current.validFrom && prevTimestamp == current.sequenced)
            throw new IllegalArgumentException(
              s"Received different effective times for the same sequenced time: previous=$prevValidFrom, current: ${current.validFrom} from event $current"
            )

          if (prevValidFrom == current.validFrom && prevTimestamp != current.sequenced)
            throw new IllegalArgumentException(
              s"Received different sequenced times for the same effective time: previous=$prevTimestamp, current: ${current.sequenced} from event $current"
            )

          ((current.sequenced, current.validFrom), current)
        },
        onComplete = _ => None,
      )

    if (!topologyConfig.validateInitialTopologySnapshot) {
      logger.info("Skipping initial topology snapshot validation")

      val directInsertGraph =
        sourceWithCheckedTimestamps
          // TODO(#34655): pass a close context to this method and add a killswitch here that gets pulled on close
          //               (be careful to cleanup the "scheduling" of pulling the killswitch in via OnShutdownRunner, ...)
          // NOTE: bulkInsert numbers the transactions with the same validFrom from 0 in every call,
          // so never split them across calls
          .groupedAdjacentBy(_.sequenced)
          .groupedWeighted(batchingConfig.maxTopologyUpdateBatchSize.value.toLong)(_.size.toLong)
          .map(_.flatten)
          .mapAsyncUS(1) { txs =>
            txs.headOption.foreach(emitProgress(lastUpdate))
            store.bulkInsert(StoredTopologyTransactions(txs))
          }
          .toMat(Sink.lastOption)(Keep.right)

      for {
        _ <- EitherT.right(store.deleteAllData())
        _ <- EitherT.right(runGraph(directInsertGraph))
      } yield ()
    } else {
      logger.info(
        s"Validating transactions to initialize the topology store ${store.storeId}"
      )

      val validatingGraph =
        sourceWithCheckedTimestamps
          // TODO(#34655): pass a close context to this method and add a killswitch here that gets pulled on close
          //               (be careful to cleanup the "scheduling" of pulling the killswitch in via OnShutdownRunner, ...)
          .groupedAdjacentBy(_.sequenced)
          .async
          .mapAsync(1) { txs =>
            val txsNonEmpty = NonEmpty
              .from(txs)
              .getOrElse(
                throw new IllegalStateException("Grouped transactions should never be empty")
              )

            emitProgress(lastUpdate)(txsNonEmpty.head1)

            processTransactionsAtSequencedTime(
              txsNonEmpty.head1.sequenced,
              txsNonEmpty.head1.validFrom,
              txs,
            )
              // transform AbortedDueToShutdown to an exception that is
              // later recovered. We don't use mapAsyncAndDrainUS, because we want to stop as soon
              // as possible after finding an error and we don't care about fully processing in-flight messages
              .failOnShutdownToAbortException(
                s"process topology transaction at ${txsNonEmpty.head1.sequenced}"
              )
              .map { _ =>
                txsNonEmpty
              }
          }
          .async
          .statefulMapAsync((SequencedTime.MinValue, 0 /* numTransactions */ )) {
            case ((fromExclusive, numTransactions), batch) =>
              val updatedCount = numTransactions + batch.size
              // only log a progress update if the new count crossed another 1k transactions step
              if (numTransactions / 1000 < updatedCount / 1000)
                logger.info(
                  s"Validated $updatedCount topology transactions to initialize the topology store ${store.storeId}"
                )

              val lastSequencedTime = batch.last1.sequenced

              // this comparison of the input and output topology snapshots serves as a security guard/barrier
              Source(batch)
                .zip(
                  store.findEssentialStateInInterval(
                    fromExclusiveO = fromExclusive.some,
                    toInclusive = lastSequencedTime,
                    includeRejected = true,
                  )
                )
                .zipWithIndex
                .dropWhile { case ((fromInitial, fromStore), _) =>
                  // we don't do a complete == comparison, because the snapshot might contain transactions with superfluous
                  // signatures that are now filtered out. Also, due to the streaming processing, the stored transactions
                  // are now checked before the successor serial is processed, therefore validUntil cannot be checked either.
                  //
                  // As long as the hash, validFrom, isProposal and rejection status
                  // (i.e. just checking whether both transactions are rejected or not without looking at the concrete reasons)
                  // agree between initial and stored topology transaction, we accept the result.
                  equalIgnoringSignaturesAndRejectionReasonDetails(fromInitial, fromStore)
                }
                // stop after the first mismatch
                .runWith(Sink.headOption)
                .map(((lastSequencedTime, updatedCount), _))
          }
          // only propagate actual mismatches to the main stream
          .collect { case Some(mismatch) => mismatch }

      for {
        _ <- EitherT.right(store.deleteAllData())
        _ = lastUpdate.set(System.nanoTime())
        validationResult = PekkoUtil
          .runSupervised(
            validatingGraph.toMat(Sink.headOption)(Keep.right),
            "initial-topology-validation",
            (doneF: Future[?]) => doneF.isCompleted,
            reportExceptionAtInfo = UnlessShutdown.isAbortedDueToShutdownException,
          )
        mismatch <- EitherT.right(
          // restore AbortedDueToShutdown from the exception to go back to EitherT[FutureUnlessShutdown, ...]
          FutureUnlessShutdown.recoverFromAbortException(validationResult)
        )
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          mismatch
            .map { case ((fromInitial, fromStore), idx) =>
              s"""Mismatch between transactions at index $idx from the initial snapshot and the topology store:
                 |Initial: $fromInitial (hash=${fromInitial.hash})
                 |Store:   $fromStore (hash=${fromStore.hash})""".stripMargin
            }
            .toLeft(())
        )
      } yield {
        logger.info(
          s"Successfully validated and imported topology transactions into the topology store ${store.storeId}."
        )
      }
    }
  }

  /** @return
    *   `true` if both transactions are the same without comparing the signatures and without
    *   comparing the content of the rejection reason. `false` otherwise.
    */
  private def equalIgnoringSignaturesAndRejectionReasonDetails(
      a: GenericStoredTopologyTransaction,
      b: GenericStoredTopologyTransaction,
  ): Boolean = a match {
    case StoredTopologyTransaction(
          b.sequenced,
          b.validFrom,
          _ignoreValidUntil,
          SignedTopologyTransaction(
            b.transaction.transaction,
            _ignoreSignatures,
            b.transaction.isProposal,
          ),
          _ignoreRejectionReason,
        ) =>
      // two transactions are considered equal, if all of the fields above are the same,
      // and if both transactions have been rejected (for whatever reason) or neither has been rejected.
      a.rejectionReason.isEmpty == b.rejectionReason.isEmpty
    case _ => false
  }

  private def processTransactionsAtSequencedTime(
      sequenced: SequencedTime,
      effectiveTime: EffectiveTime,
      storedTxs: Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    stateProcessor
      .validateAndApplyAuthorization(
        sequenced,
        effectiveTime,
        storedTxs.map(_.transaction),
        expectFullAuthorization = false,
        // when validating the initial snapshot, missing signing key signatures are only
        // acceptable, if the transaction was part of the genesis topology snapshot
        relaxChecksForBackwardsCompatibility =
          sequenced.value == SignedTopologyTransaction.InitialTopologySequencingTime,
      )
      .map(_ => ())
}
