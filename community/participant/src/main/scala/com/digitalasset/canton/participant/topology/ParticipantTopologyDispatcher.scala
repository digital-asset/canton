// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.syntax.either._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainRegistryError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.v0.RegisterTopologyTransactionResponse
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{StoredTopologyTransaction, TopologyStore}
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, retry}
import com.digitalasset.canton.{DomainAlias, DomainId}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import com.digitalasset.canton.util.Thereafter.syntax._

sealed trait ParticipantIdentityDispatcherError

trait RegisterTopologyTransactionHandle extends FlagCloseable {
  def submit(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  ): FutureUnlessShutdown[Seq[v0.RegisterTopologyTransactionResponse.Result]]
}

/** Identity dispatcher, registering identities with a domain
  *
  * The dispatcher observes the participant topology manager and tries to shovel
  * new topology transactions added to the manager to all connected domains.
  */
class ParticipantTopologyDispatcher(
    manager: ParticipantTopologyManager,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def queueStatus: TopologyQueueStatus = {
    val (dispatcher, clients) = domains.values.foldLeft((0, 0)) { case ((disp, clts), outbox) =>
      (disp + outbox.queueSize, clts + outbox.targetClient.numPendingChanges)
    }
    TopologyQueueStatus(
      manager = manager.queueSize,
      dispatcher = dispatcher,
      clients = clients,
    )
  }

  // connect to manager
  manager.addObserver(new ParticipantTopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val mapped = transactions.map(tx => StoredTopologyTransaction(timestamp, None, tx))
      domains.values.toList.traverse(_.enqueue(mapped)).map(_ => ())
    }
  })

  private val domains = new TrieMap[DomainAlias, DomainOutbox]()

  def domainDisconnected(domain: DomainAlias): Future[Unit] = {
    def run(): Future[Unit] = {
      domains.remove(domain) match {
        case Some(outbox) =>
          // close handle before outbox so handle can immediately stop waiting for responses
          // and frees outbox to also complete closing itself
          outbox.handle.close()
          outbox.close()
        case None =>
      }
      Future.unit
    }
    // running synchronized to ensure we don't race with subsequent additions
    manager.sequentialStoreRead(run(), s"disconnect from $domain")
  }

  /** signal domain connection
    *
    * @param pushAndClose if set to true, we will push the current state to the domain but
    *                     immediately afterwards disconnect again.
    *                     this is used to avoid race conditions between connect / disconnect
    *                     during initial onboarding
    */
  def domainConnected(
      domain: DomainAlias,
      domainId: DomainId,
      handle: RegisterTopologyTransactionHandle,
      client: DomainTopologyClientWithInit,
      targetStore: TopologyStore,
      pushAndClose: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, Unit]] = {

    val promise = Promise[UnlessShutdown[Either[DomainRegistryError, Unit]]]()

    def notifyDomainHandshake(dispatcher: FutureUnlessShutdown[Unit]): Unit =
      FutureUtil.doNotAwait(
        dispatcher.unwrap.transform { result =>
          promise.success(
            result.sequence
              .map(
                _.toEither
                  .leftMap(
                    DomainRegistryError.DomainRegistryInternalError.IdentityHandshakeError(_)
                  )
              )
          )
          Success(())
        },
        "Notify domain handshake failed",
      )

    // sending the current snapshot is necessary as the domain handshake requires the topology transactions
    // to be registered with the domain before proceeding
    lazy val run = performUnlessClosingF {
      if (!domains.contains(domain)) {
        val outbox = new DomainOutbox(
          domain,
          domainId,
          handle,
          client,
          manager.store,
          targetStore,
          timeouts,
          loggerFactory.appendUnnamedKey("domain outbox", domain.unwrap),
        )
        // register new outbox if this isn't just push and close
        if (!pushAndClose) {
          domains += domain -> outbox
        } else {
          // otherwise ensure we close this outbox after succeeding
          promise.future.thereafter { _ =>
            outbox.handle.close()
            outbox.close()
          }
        }
        outbox.recomputeQueue.map { _ =>
          // start the pushing. this will first copy and clear the queue
          val resF = outbox.flush()
          notifyDomainHandshake(resF)
          // now that the queue is copied / cleared, we vacate the sequential read thread
        }
      } else {
        Future.unit
      }
    }.onShutdown(())
    // running this on the managers sequential processing queue to avoid race conditions
    manager.sequentialStoreRead(run, s"connect to $domain")
    FutureUnlessShutdown(promise.future)
  }

}

private class DomainOutbox(
    domain: DomainAlias,
    domainId: DomainId,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: DomainTopologyClientWithInit,
    source: TopologyStore,
    target: TopologyStore,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  private val queue = ListBuffer[StoredTopologyTransaction[TopologyChangeOp]]()
  private val running = new AtomicBoolean(false)

  def queueSize: Int = {
    queue.size + (if (running.get()) 1 else 0)
  }

  def enqueue(
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = performUnlessClosingF {
    onlyApplicable(transactions).map { filtered =>
      synchronized {
        queue ++= filtered
        kickOffFlush()
      }
    }
  }

  private def onlyApplicable(
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredTopologyTransaction[TopologyChangeOp]]] = {
    def notAlien(mapping: TopologyMapping): Boolean = mapping match {
      case OwnerToKeyMapping(_: ParticipantId, _) => true
      case OwnerToKeyMapping(owner, _) => owner.uid == domainId.unwrap
      case _ => true
    }
    def filter(
        stored: StoredTopologyTransaction[TopologyChangeOp]
    ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]] = {
      // ensure we only forward transactions suitable to this domain
      val include = stored.transaction.restrictedToDomain.forall(_ == domainId) &&
        // ensure that we don't try to register alien domain entities
        notAlien(stored.transaction.transaction.element.mapping)
      if (!include)
        Future.successful(None)
      else
        target.exists(stored.transaction).map(exists => Option.when(!exists)(stored))
    }
    transactions.toList.traverseFilter(filter)
  }

  private def kickOffFlush()(implicit traceContext: TraceContext): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    FutureUtil.doNotAwait(flush().unwrap, "domain outbox flusher")
  }

  def recomputeQueue(implicit traceContext: TraceContext): Future[Unit] = {
    queue.clear()
    for {
      // find the current target watermark
      watermarkTsO <- target.currentDispatchingWatermark
      watermarkTs = watermarkTsO.getOrElse(CantonTimestamp.MinValue)
      // fetch all transactions in the authorized store that have not yet been dispatched
      dispatchingCandidates <- source.findDispatchingTransactionsAfter(watermarkTs)
      // filter candidates for domain
      filtered <- onlyApplicable(dispatchingCandidates.result)
    } yield {

      ErrorUtil.requireState(queue.isEmpty, s"Queue contains ${queue.size} unexpected elements.")
      queue.appendAll(filtered)
      logger.debug(
        s"Resuming dispatching with ${filtered.length} transactions after watermark $watermarkTs"
      )
    }
  }

  def flush()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    performUnlessClosingF {
      if (!running.getAndSet(true)) {
        val ret = synchronized {
          val ret = queue.result()
          queue.clear()
          ret
        }
        if (ret.nonEmpty) {
          val dp = dispatch(domain, handle, transactions = ret)
          dp.transform {
            case Failure(exception) =>
              logger.warn(s"Pushing ${ret.length} transactions to domain failed.", exception)
              // put stuff back into queue
              synchronized {
                queue.insertAll(0, ret)
              }
              running.set(false)
              kickOffFlush() // kick off new flush in the background
              Success(())
            case _ =>
              running.set(false)
              kickOffFlush()
              Success(())
          }
        } else {
          running.set(false)
          Future.unit
        }
      } else {
        Future.unit
      }
    }
  }

  def updateWatermark(
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp]],
      responses: Seq[RegisterTopologyTransactionResponse.Result],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF {
      val timestampMap =
        transactions.map(x => (x.transaction.uniquePath.toProtoPrimitive, x.validFrom)).toMap
      // in theory, we should receive as many responses as we have submitted transactions
      // but we update the watermark in a way that should work even if there is some issue on the domain
      val newWatermarkO = responses.foldLeft(None: Option[CantonTimestamp]) { case (acc, elem) =>
        timestampMap
          .get(elem.uniquePath)
          .fold {
            logger.error(
              s"Received result for ${elem.uniquePath} but I did not submit this transaction as part of this request"
            )
            acc
          } { ts =>
            // this should work even if our response is out of order (as long as the ts are unique)
            Some(acc.fold(ts)(existing => CantonTimestamp.max(existing, ts)))
          }
      }
      newWatermarkO.fold {
        logger.error(
          s"Not updating watermark as submitted transactions ${transactions} did not yield a corresponding result $responses"
        )
        Future.unit
      } { newWatermark =>
        if (queue.headOption.exists(x => x.validFrom <= newWatermark)) {
          // once we add batching on transaction addition to the authorized store, we might get multiple tx with the
          // same timestamp. then we need to get a bit smarter. leaving this warning here in case we miss fixing this
          logger.warn(
            s"Transaction with same or earlier timestamp is still queued. Not updating watermark "
          )
          Future.unit
        } else {
          // update the watermark. we only use it when we restart the queue (so we don't go back too much in the history)
          target.updateDispatchingWatermark(newWatermark)
        }
      }
    }

  private def dispatch(
      domain: DomainAlias,
      handle: RegisterTopologyTransactionHandle,
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    implicit val success = retry.Success.always
    retry
      .Backoff(
        logger,
        this,
        timeouts.unbounded.retries(1.second),
        1.second,
        10.seconds,
        "push topology transaction",
      )
      .unlessShutdown(
        {
          logger.debug(s"Attempting to push ${transactions.size} topology transactions to $domain")
          FutureUtil.logOnFailureUnlessShutdown(
            handle.submit(transactions.map(_.transaction)),
            s"Pushing topology transactions to $domain",
          )
        },
        AllExnRetryable,
      )
      .flatMap { responses =>
        val (failedResponses, otherResponses) =
          responses.partition(
            _.state == v0.RegisterTopologyTransactionResponse.Result.State.FAILED
          )

        if (failedResponses.nonEmpty)
          logger.error(
            s"$domain responded with failure for the given topology transactions: $failedResponses"
          )

        if (otherResponses.nonEmpty)
          logger.debug(
            s"$domain responded the following for the given topology transactions: $otherResponses"
          )
        updateWatermark(transactions, responses)
      }
      .onShutdown(())
  }
}
