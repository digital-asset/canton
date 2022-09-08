// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.definitions.LedgerApiErrors.ConsistencyErrors.ContractNotFound
import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.daml.ledger.api.v1.commands.{Command => ScalaCommand}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.client.binding.{Contract, Primitive => P}
import com.digitalasset.canton.error.ErrorCodeUtils
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.workflows.{PingPong => M}
import com.digitalasset.canton.participant.ledger.api.client.CommandSubmitterWithRetry.Failed
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandSubmitterWithRetry,
  DecodeUtil,
  LedgerSubmit,
}
import com.digitalasset.canton.protocol.messages.LocalReject.ConsistencyRejections.{
  InactiveContracts,
  LockedContracts,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration.DurationLong
import scala.math.min
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Implements the core of the ledger ping service for a participant.
  * The service measures the time needed for a nanobot on the responder to act on a contract created by the initiator.
  *
  * The main functionality:
  * 1. once instantiated, it automatically starts a Scala Nanobot that responds to pings for this participant
  * 2. it provides a ping method that sends a ping to the given (target) party
  *
  * Parameters:
  * @param adminParty P.Party              the party on whose behalf to send/respond to pings
  * @param maxLevelSupported Long          the maximum level we will participate in "Explode / Collapse" Pings
  * @param loggerFactory NamedLogger       logger
  * @param clock Clock                     clock for regular garbage collection of duplicates and merges
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PingService(
    connection: LedgerSubmit,
    adminParty: P.Party,
    maxLevelSupported: Long,
    pingDeduplicationTime: NonNegativeFiniteDuration,
    isActive: => Boolean,
    protected val loggerFactory: NamedLoggerFactory,
    protected val clock: Clock,
)(implicit ec: ExecutionContext, timeoutScheduler: ScheduledExecutorService)
    extends AdminWorkflowService
    with NamedLogging {
  // TODO(#6318): Once we have an ACS service, it should also use it to vacuum stale pings/pongs upon connect

  // Used to synchronize the ping requests and responses.
  // Once the promise is fulfilled, the ping for the given id is complete.
  // The result String contains of the future yields the party that responded. The string indicates if a
  // response was already received from a certain sender. We use this to check within a grace period for "duplicate spends".
  // So when a response was received, we don't end the future, but we wait for another `grace` period if there is
  // a second, invalid response.
  private val responses: TrieMap[String, (Option[String], Option[Promise[String]])] = new TrieMap()

  private case class DuplicateIdx(pingId: String, keyId: String)
  private val duplicate = TrieMap.empty[DuplicateIdx, (Unit => String)]

  private case class MergeIdx(pingId: String, path: String)
  private case class MergeItem(merge: Contract[M.Merge], first: Option[Contract[M.Collapse]])
  private val merges: TrieMap[MergeIdx, MergeItem] = new TrieMap()

  private val DefaultCommandTimeoutMillis: Long = 5 * 60 * 1000

  /** The command deduplication time for commands that don't need deduplication because
    * any repeated submission would fail anyway, say because the command exercises a consuming choice on
    * a specific contract ID (not contract key).
    */
  private def NoCommandDeduplicationNeeded: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(1)

  /** Send a ping to the target party, return round-trip time or a timeout
    * @param targetParties String     the parties to send ping to
    * @param validators additional validators (signatories) of the contracts
    * @param timeoutMillis Long     how long to wait for pong (in milliseconds)
    */
  def ping(
      targetParties: Set[String],
      validators: Set[String],
      timeoutMillis: Long,
      duplicateGracePeriod: Long = 1000,
      maxLevel: Long = 0,
      workflowId: Option[WorkflowId] = None,
      id: String = UUID.randomUUID().toString,
  )(implicit traceContext: TraceContext): Future[PingService.Result] = {
    logger.debug(s"Sending ping $id from $adminParty to $targetParties")
    val promise = Promise[String]()
    val resultPromise: (Option[String], Option[Promise[String]]) =
      (None, Some(promise))
    responses += (id -> resultPromise)
    if (maxLevel > maxLevelSupported) {
      logger.warn(s"Capping max level $maxLevel to $maxLevelSupported")
    }
    val start = System.nanoTime()
    val result = for {
      _ <- submitPing(
        id,
        targetParties,
        validators,
        min(maxLevel, maxLevelSupported),
        workflowId,
        timeoutMillis,
      )
      response <- timeout(promise.future, timeoutMillis)
      end = System.nanoTime()
      rtt = Duration.ofNanos(end - start)
      _ = if (targetParties.size > 1)
        logger.debug(s"Received ping response from $response within $rtt")
      successMsg = PingService.Success(rtt, response)
      success <- responses.get(id) match {
        case None =>
          // should not happen as this here is the only place where we remove them
          Future.failed(new RuntimeException("Ping disappeared while waiting"))
        case Some((Some(`response`), Some(gracePromise))) =>
          // wait for grace period
          if (targetParties.size > 1) {
            // wait for grace period to expire. if it throws, we are good, because we shouldn't get another responsefailed with reason
            timeout(gracePromise.future, duplicateGracePeriod) transform {
              // future needs to timeout. otherwise we received a duplicate spent
              case Failure(_: TimeoutException) => Success(successMsg)
              case Success(x) =>
                logger.debug(s"gracePromise for Ping $id resulted in $x. Expected a Timeout.")
                Success(PingService.Failure)
              case Failure(ex) =>
                logger.debug(s"gracePromise for Ping $id threw unexpected exception.", ex)
                Failure(ex)
            }
          } else Future.successful(successMsg)
        case Some(x) =>
          logger.debug(s"Ping $id response was $x. Expected (Some, Some).")
          Future.successful(PingService.Failure)
      }
      _ = logger.debug(s"Ping test $id resulted in $success, deregistering")
      _ = responses -= id
    } yield success

    result transform {
      case Failure(_: TimeoutException) =>
        responses -= id
        Success(PingService.Failure)
      case other => other
    }
  }

  override def close(): Unit = {
    Lifecycle.close(connection)(logger)
  }

  private def submitPing(
      id: String,
      responders: Set[String],
      validators: Set[String],
      maxLevel: Long,
      workflowId: Option[WorkflowId],
      timeoutMillis: Long,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    if (validators.isEmpty) {
      logger.debug(s"Starting ping $id with responders $responders and level $maxLevel")
      val ping = M.Ping(id, adminParty, List.empty, responders.map(P.Party(_)).toList, maxLevel)
      submitIgnoringErrors(
        id,
        "ping",
        ping.create.command,
        workflowId,
        pingDeduplicationTime,
        timeoutMillis,
      )
    } else {
      logger.debug(
        s"Proposing ping $id with responders $responders, validators $validators and level $maxLevel"
      )
      val ping = M.PingProposal(
        id = id,
        initiator = adminParty,
        candidates = validators.map(P.Party(_)).toList,
        validators = List.empty,
        responders = responders.map(P.Party(_)).toList,
        maxLevel = maxLevel,
      )
      submitIgnoringErrors(
        id,
        "ping-proposal",
        ping.create.command,
        workflowId,
        pingDeduplicationTime,
        timeoutMillis,
      )
    }
  }

  @nowarn("msg=match may not be exhaustive")
  private def submitIgnoringErrors(
      id: String,
      action: String,
      cmd: ScalaCommand,
      workflowId: Option[WorkflowId],
      deduplicationDuration: NonNegativeFiniteDuration,
      timeoutMillis: Long = DefaultCommandTimeoutMillis,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val desc = s"Ping/pong with id=$id-$action"
    // Include a random UUID in the command id, as id/action are not necessarily unique.
    // - The same participant may submit several pings with the same ID.
    // - A ping may get duplicated by CommandSubmitterWithRetry
    val commandId = s"$id-$action-${UUID.randomUUID()}"

    timeout(
      connection.submitCommand(
        Seq(cmd),
        Some(commandId),
        workflowId,
        deduplicationTime = Some(deduplicationDuration),
      ),
      timeoutMillis,
    ).transform { res =>
      res match {
        case Failure(_: TimeoutException) =>
          // Info log level as we sometimes do a ping expecting that it will fail.
          logger.info(s"$desc: no completion received within timeout of ${timeoutMillis.millis}.")
        case Failure(ex) if NonFatal(ex) =>
          logger.warn(s"$desc failed due to an internal error.", ex)
        case Success(CommandSubmitterWithRetry.Success(_)) =>
          logger.debug(s"$desc succeeded.")
        case Success(Failed(completion)) if completion.status.exists { s =>
              ErrorCodeUtils.isError(s.message, ContractNotFound) ||
              ErrorCodeUtils.isError(s.message, InactiveContracts) ||
              ErrorCodeUtils.isError(s.message, LockedContracts)
            } =>
          logger.info(s"$desc failed with reason ${completion.status}.")
        case Success(reason) =>
          logger.warn(s"$desc failed with reason $reason.")
      }
      Success(())
    }
  }

  private def submitAsync(
      id: String,
      action: String,
      cmd: ScalaCommand,
      workflowId: Option[WorkflowId],
      deduplicationDuration: NonNegativeFiniteDuration,
      timeoutMillis: Long = DefaultCommandTimeoutMillis,
  )(implicit traceContext: TraceContext): Unit =
    FutureUtil.doNotAwait(
      submitIgnoringErrors(id, action, cmd, workflowId, deduplicationDuration, timeoutMillis),
      s"failed to react to $id with $action",
    )

  override private[admin] def processTransaction(
      tx: Transaction
  )(implicit traceContext: TraceContext): Unit = {
    // Process ping transactions only on the active replica
    if (isActive) {
      val workflowId = WorkflowId(tx.workflowId)
      processPings(DecodeUtil.decodeAllCreated(M.Ping)(tx), workflowId)
      processPongs(DecodeUtil.decodeAllCreated(M.Pong)(tx), workflowId)
      processExplode(DecodeUtil.decodeAllCreated(M.Explode)(tx), workflowId)
      processMerge(DecodeUtil.decodeAllCreated(M.Merge)(tx))
      processCollapse(DecodeUtil.decodeAllCreated(M.Collapse)(tx), workflowId)
      processProposal(DecodeUtil.decodeAllCreated(M.PingProposal)(tx), workflowId)
    }
  }

  private def duplicateCheck[T](pingId: String, uniqueId: String, contract: Any)(implicit
      traceContext: TraceContext
  ): Unit = {
    val key = DuplicateIdx(pingId, uniqueId)
    // store contract for later check
    duplicate.get(key) match {
      case None => duplicate += key -> (_ => contract.toString)
      case Some(other) =>
        logger.error(s"Duplicate contract observed for ping-id $pingId: $contract vs $other")
    }
  }

  protected def processProposal(proposals: Seq[Contract[M.PingProposal]], workflowId: WorkflowId)(
      implicit traceContext: TraceContext
  ): Unit = {
    // accept proposals where i'm the next candidate
    proposals.filter(_.value.candidates.headOption.contains(adminParty)).foreach { proposal =>
      logger.debug(s"Accepting ping proposal ${proposal.value.id} from ${proposal.value.initiator}")
      val command = proposal.contractId.exerciseAccept(adminParty).command
      submitAsync(
        proposal.value.id,
        "ping-proposal-accept",
        command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
      )
    }
  }

  protected def processExplode(explodes: Seq[Contract[M.Explode]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit =
    explodes.filter(_.value.responders.contains(adminParty)).foreach { p =>
      duplicateCheck(p.value.id, "explode" + p.value.path, p)
      logger
        .debug(s"$adminParty processing explode of id ${p.value.id} with path ${p.value.path}")
      submitAsync(
        p.value.id,
        "explode" + p.value.path,
        p.contractId.exerciseProcessExplode(adminParty).command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
      )
    }

  protected def processMerge(
      contracts: Seq[Contract[M.Merge]]
  )(implicit traceContext: TraceContext): Unit = {
    contracts
      .filter(_.value.responders.contains(adminParty))
      .foreach { p =>
        duplicateCheck(p.value.id, "merge" + p.value.path, p)
        logger.debug(s"$adminParty storing merge of ${p.value.id} with path ${p.value.path}")
        merges += MergeIdx(p.value.id, p.value.path) -> MergeItem(p, None)
      }
  }

  protected def processCollapse(contracts: Seq[Contract[M.Collapse]], workflowId: WorkflowId)(
      implicit traceContext: TraceContext
  ): Unit = {

    def addOrCompleteCollapse(
        index: MergeIdx,
        item: PingService.this.MergeItem,
        contract: Contract[M.Collapse],
    ): Unit = {
      val id = contract.value.id
      val path = contract.value.path
      item.first match {
        case None =>
          logger.debug(s"$adminParty observed first collapsed for id $id and path $path")
          merges.update(index, item.copy(first = Some(contract)))
        case Some(other) =>
          logger.debug(
            s"$adminParty observed second collapsed for id $id and path $path. Collapsing."
          )
          merges.remove(index)
          // We intentionally don't return the future here, as we just submit the command here and do timeout tracking
          // explicitly with the timeout scheduler.
          submitAsync(
            item.merge.value.id,
            s"collapse-${item.merge.value.path}",
            item.merge.contractId
              .exerciseProcessMerge(adminParty, other.contractId, contract.contractId)
              .command,
            Some(workflowId),
            NoCommandDeduplicationNeeded,
          )
      }
    }

    contracts
      .filter(_.value.responders.contains(adminParty))
      .foreach(p => {
        val index = MergeIdx(p.value.id, p.value.path)
        merges.get(index) match {
          case None => logger.error(s"Received collapse for processed merge: $p")
          case Some(item) => addOrCompleteCollapse(index, item, p)
        }
      })
  }

  private def processPings(pings: Seq[Contract[M.Ping]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit = {
    def processPing(p: Contract[M.Ping]): Unit = {
      logger.info(s"$adminParty responding to a ping from ${P.Party.unwrap(p.value.initiator)}")
      submitAsync(
        p.value.id,
        "respond",
        p.contractId.exerciseRespond(adminParty).command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
      )
      scheduleGarbageCollection(p.value.id)
    }
    pings.filter(_.value.responders.contains(adminParty)).foreach(processPing)
  }

  private def scheduleGarbageCollection(id: String)(implicit traceContext: TraceContext): Unit = {
    // remove items from duplicate filter
    val scheduled = clock.scheduleAfter(
      _ => {
        val filteredDuplicates = duplicate.filter(x => x._1.pingId != id)
        if (filteredDuplicates.size != duplicate.size) {
          logger.debug(
            s"Garbage collecting ${duplicate.size - filteredDuplicates.size} elements from duplicate filter"
          )
        }
        duplicate.clear()
        duplicate ++= filteredDuplicates

        val filteredMerges = merges.filter(x => x._1.pingId != id)
        // TODO(rv): abort ping and cleanup ledger
        merges.clear()
        merges ++= filteredMerges
      },
      Duration.ofSeconds(1200),
    )
    FutureUtil.doNotAwait(scheduled.unwrap, "failed to schedule garbage collection")
  }

  @VisibleForTesting
  private[admin] def processPongs(pongs: Seq[Contract[M.Pong]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit =
    pongs.filter(_.value.initiator == adminParty).foreach { p =>
      // purge duplicate checker
      duplicate.clear()
      // first, ack the pong
      val responder = P.Party.unwrap(p.value.responder)
      logger.info(s"$adminParty received pong from $responder")
      val submissionF = for {
        _ <- submitIgnoringErrors(
          p.value.id,
          "ack",
          p.contractId.exerciseAck().command,
          Some(workflowId),
          NoCommandDeduplicationNeeded,
        )
      } yield {
        val id = p.value.id
        responses.get(id) match {
          case None =>
            logger.debug(s"Received response for un-expected ping $id from $responder")
          // receive first (and only correct) pong (we don't now sender yet)
          case Some((None, Some(promise))) =>
            val newPromise = Promise[String]()
            responses.update(id, (Some(responder), Some(newPromise)))
            logger.debug(s"Notifying user about success of ping $id")
            promise.success(responder)
          // receive subsequent pong () which means that we have a duplicate spent!
          case Some((Some(first), Some(promise))) =>
            logger.error(
              s"Received duplicate response for $id from $responder, already received from $first"
            )
            // update responses so we don't fire futures twice even if there are more subsequent pongs
            responses.update(id, (Some(first), None))
            // it's not a success, but the future succeeds
            promise.success(responder)
          // receive even more pongs
          case Some((Some(first), None)) =>
            logger.error(
              s"Received even more responses for $id from $responder, already received from $first"
            )
          // error
          case Some(_) =>
            logger.error(s"Invalid state observed! ${responses.get(id)}")
        }
      }
      FutureUtil.doNotAwait(submissionF, s"failed to process pong for ${p.value.id}")
    }

  /** Races the supplied future against a timeout.
    * If the supplied promise completes first the returned future will be completed with this value.
    * If the timeout completes first the returned Future will fail with a [[PingService.TimeoutException]].
    * If a timeout occurs no attempt is made to cancel/stop the provided future.
    */
  private def timeout[T](other: Future[T], durationMillis: Long)(implicit
      traceContext: TraceContext
  ): Future[T] = {
    val result = Promise[T]()

    other.onComplete(result.tryComplete)
    // schedule completing the future exceptionally if it didn't finish before the timeout deadline
    timeoutScheduler.schedule(
      { () =>
        {
          if (result.tryFailure(new TimeoutException))
            logger.info(s"Operation timed out after $durationMillis millis.")
        }
      }: Runnable,
      durationMillis,
      TimeUnit.MILLISECONDS,
    )

    result.future
  }

  private class TimeoutException extends RuntimeException("Future has timed out")
}

object PingService {

  sealed trait Result
  final case class Success(roundTripTime: Duration, responder: String) extends Result
  object Failure extends Result

}
