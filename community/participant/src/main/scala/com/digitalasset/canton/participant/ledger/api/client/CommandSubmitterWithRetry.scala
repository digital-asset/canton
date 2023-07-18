// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import akka.NotUsed
import akka.stream.QueueOfferResult.{Dropped, Enqueued, Failure, QueueClosed}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, MergePreferred, Sink, Source}
import akka.stream.{FlowShape, KillSwitches, Materializer, OverflowStrategy}
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.nameof.NameOf.functionFullName
import com.daml.util.Ctx
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error
import com.digitalasset.canton.ledger.client.services.commands.CommandSubmission
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.ledger.api.client.CommandSubmitterWithRetry.{
  CommandResult,
  CommandsCtx,
  Failed,
  retryCommandFlow,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.opentelemetry.api.trace.Tracer

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Wraps a command tracker with the ability to retry timed out commands up to a given max number of retries
  */
class CommandSubmitterWithRetry(
    maxRetries: Int,
    cmdTracker: Flow[Ctx[CommandsCtx, CommandSubmission], Ctx[CommandsCtx, Completion], _],
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
)(implicit mat: Materializer, ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with FlagCloseableAsync {
  private val retryFlow = retryCommandFlow(maxRetries, cmdTracker, logger, overrideRetryable)
  @nowarn("msg=method dropNew in object OverflowStrategy is deprecated")
  private val source =
    Source.queue[Ctx[CommandsCtx, CommandSubmission]](
      bufferSize = 10000,
      overflowStrategy = OverflowStrategy.dropNew,
    )
  private val ((queue, killSwitch), done) = {
    import TraceContext.Implicits.Empty.*

    AkkaUtil.runSupervised(
      logger.error("Fatally failed to handle retry flow", _),
      source
        .via(retryFlow)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.both),
    )
  }

  /** Submits commands and retries timed out ones (at most the amount given by `maxRetries`)
    * @param commands to be submitted
    * @return Future with the result of the submission. The result can signal success, failure or max retries reached
    */
  def submitCommands(
      commands: Commands
  )(implicit traceContext: TraceContext): Future[CommandResult] = {
    val resultPromise = Promise[CommandResult]()
    val ctx =
      Ctx(
        CommandsCtx(commands, resultPromise),
        CommandSubmission(commands),
        traceContext.toDamlTelemetryContext,
      )
    for {
      queueOfferResult <-
        // The performUnlessClosing has been introduced
        // because we have observed exceptions in case a command was submitted after shutdown.
        performUnlessClosingF(functionFullName) { queue.offer(ctx) }.unwrap

      result <- queueOfferResult match {
        case UnlessShutdown.Outcome(Enqueued) => resultPromise.future
        case UnlessShutdown.Outcome(Failure(cause)) => Future.failed(cause)
        case UnlessShutdown.Outcome(Dropped) =>
          Future.successful(
            Failed(
              Completion()
                .withCommandId(commands.commandId)
                .withStatus(
                  Status()
                    .withCode(Code.ABORTED.value)
                    .withMessage("Client side backpressure on command submission")
                )
            )
          )
        case UnlessShutdown.Outcome(QueueClosed) | UnlessShutdown.AbortedDueToShutdown =>
          logger.info(
            s"Rejected command ${commands.commandId} as the participant is shutting down."
          )
          val status =
            Status(
              code = Code.UNAVAILABLE.value,
              message = "Command rejected, as the participant is shutting down.",
            )
          val completion = Completion(commandId = commands.commandId, status = Some(status))
          Future.successful(Failed(completion))
      }
    } yield result
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    List[AsyncOrSyncCloseable](
      SyncCloseable("queue.complete", queue.complete()),
      SyncCloseable("killSwitch.shutdown", killSwitch.shutdown()),
      AsyncCloseable("queue.completion", queue.watchCompletion(), timeouts.shutdownShort.unwrap),
      AsyncCloseable("done", done, timeouts.shutdownShort.unwrap),
    )
  }
}

object CommandSubmitterWithRetry {
  sealed trait CommandResult extends PrettyPrinting

  final case class Success(completion: Completion) extends CommandResult {
    override def pretty: Pretty[Success.this.type] = prettyOfClass(unnamedParam(_.completion))
  }

  final case class Failed(completion: Completion) extends CommandResult {
    override def pretty: Pretty[Failed] = prettyOfClass(unnamedParam(_.completion))
  }

  final case class MaxRetriesReached(completion: Completion) extends CommandResult {
    override def pretty: Pretty[MaxRetriesReached] = prettyOfClass(unnamedParam(_.completion))
  }

  final case class CommandsCtx(
      commands: Commands,
      promise: Promise[CommandResult],
      retries: Int = 0,
  )

  def retryCommandFlow(
      maxRetries: Int,
      cmdTracker: Flow[Ctx[CommandsCtx, CommandSubmission], Ctx[CommandsCtx, Completion], _],
      logger: TracedLogger,
      overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
  ): Flow[Ctx[CommandsCtx, CommandSubmission], Ctx[CommandsCtx, CommandSubmission], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits.*

      val merge =
        b.add(MergePreferred[Ctx[CommandsCtx, CommandSubmission]](1, eagerComplete = true))
      val bcast = b.add(Broadcast[Ctx[CommandsCtx, CommandSubmission]](2))

      (merge ~> cmdTracker
        .map(stopOrRetry(maxRetries, logger, overrideRetryable))
        .collect { case Some(ctx) =>
          ctx
        } ~> bcast).discard

      bcast ~> merge.preferred // retry

      FlowShape(merge.in(0), bcast.out(1))
    })

  def stopOrRetry(
      maxRetries: Int,
      logger: TracedLogger,
      overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
  )(c: Ctx[CommandsCtx, Completion]): Option[Ctx[CommandsCtx, CommandSubmission]] = {
    import TraceContext.Implicits.Empty.*
    val statusO = c.value.status
    val result: Either[CommandResult, CommandsCtx] = statusO match {
      case None =>
        logger.info(s"Command with id = ${c.value.commandId} failed without a status. Giving up.")
        Left(Failed(c.value))
      case Some(status) =>
        if (isSuccess(status)) {
          logger.debug(s"Command with id = ${c.value.commandId} completed successfully.")
          Left(Success(c.value))
        } else if (!isRetryable(status, overrideRetryable)) {
          logger.info(
            s"Command with id = ${c.value.commandId} failed with status $status. Giving up."
          )
          Left(Failed(c.value))
        } else if (c.context.retries >= maxRetries) {
          logger.info(
            s"Command with id = ${c.value.commandId} failed after reaching max retries of $maxRetries with status $status."
          )
          Left(MaxRetriesReached(c.value))
        } else {
          val newRetries = c.context.retries + 1
          val newCtxt = c.context.copy(retries = newRetries)
          logger.info(
            s"Command with id = ${c.value.commandId} failed with status $status. Retrying (attempt: $newRetries)."
          )
          Right(newCtxt)
        }
    }

    result.fold(
      result => {
        c.context.promise.success(result)
        None
      },
      ctx => Some(Ctx(ctx, CommandSubmission(ctx.commands), c.telemetryContext)),
    )
  }

  def isSuccess(status: Status): Boolean = status.code == Code.OK.value

  def isRetryable(
      status: Status,
      overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
  ): Boolean =
    overrideRetryable.applyOrElse(
      status,
      (s: Status) =>
        error.ErrorCodeUtils.errorCategoryFromString(s.message).exists(_.retryable.nonEmpty),
    )

}
