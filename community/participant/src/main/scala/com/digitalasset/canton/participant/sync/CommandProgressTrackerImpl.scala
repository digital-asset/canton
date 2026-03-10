// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandState,
  CommandUpdates,
  Contract,
  RequestStatistics,
}
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.value.Identifier
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig.{
  defaultMaxFailed,
  defaultMaxPending,
  defaultMaxSucceeded,
}
import com.digitalasset.canton.participant.sync.CommandProgressTrackerImpl.*
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandProgressTracker,
  CommandResultHandle,
  CommandStatus,
}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.CompletionFromTransaction.CommonCompletionProperties
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.{LfSubmittedTransaction, RootHash}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.Mutex
import com.digitalasset.daml.lf.data.Ref.TypeConId
import com.digitalasset.daml.lf.transaction.Node.LeafOnlyAction
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, Node}
import io.grpc.StatusRuntimeException
import monocle.macros.syntax.lens.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

final case class CommandProgressTrackerConfig(
    enabled: Boolean = true,
    maxFailed: NonNegativeInt = defaultMaxFailed,
    maxPending: NonNegativeInt = defaultMaxPending,
    maxSucceeded: NonNegativeInt = defaultMaxSucceeded,
)

object CommandProgressTrackerConfig {
  lazy val defaultMaxFailed: NonNegativeInt = NonNegativeInt.tryCreate(100)
  lazy val defaultMaxPending: NonNegativeInt = NonNegativeInt.tryCreate(1000)
  lazy val defaultMaxSucceeded: NonNegativeInt = NonNegativeInt.tryCreate(100)
}

@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
class CommandProgressTrackerImpl(
    config: CommandProgressTrackerConfig,
    clock: Clock,
    metrics: Timer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandProgressTracker
    with NamedLogging {

  private class MyCommandResultHandle(
      key: CommandKey,
      val status: AtomicReference[CommandStatus],
  ) extends CommandResultHandle {

    private val lastStageAndTime =
      new AtomicReference[(OrderedStage, Long)]((Phase_Begin, System.nanoTime()))

    private def updateWithStatus(
        err: com.google.rpc.status.Status,
        state: CommandState,
    ): Unit =
      status.updateAndGet { x =>
        x
          .focus(_.completion.status)
          .replace(Some(err))
          .copy(
            state = state,
            completed = Some(clock.now),
          )
      }.discard

    def recordProgress(stage: OrderedStage): Unit = {
      val cur = System.nanoTime()
      val (prevStage, prevTime) = lastStageAndTime.getAndUpdate {
        case (prevStage, _) if prevStage.ord < stage.ord =>
          (stage, cur)
        case other =>
          other
      }
      // update if we truly progressed (is not guaranteed as we might e.g. receive
      // a verdict twice if we have two mediators running with threshold = 1
      if (prevStage.ord < stage.ord) {
        status
          .updateAndGet(x =>
            x.copy(timings =
              (
                prevStage.description,
                TimeUnit.NANOSECONDS.toMillis(cur - prevTime).toInt,
              ) +: x.timings
            )
          )
          .discard
        // only update timing if the timing made sense. in some cases we might
        // not run conflict detection (as there is nothing to detect) and so we don't
        // want our stats to be distorted
        if (prevStage.ord + 1 == stage.ord) {
          val cmd = status.get()
          val context =
            cmd.synchronizerId.map(c => ("synchronizer" -> c.uid.identifier.str)).toList ++ Seq(
              "phase" -> stage.description
            )
          metrics.update(TimeUnit.NANOSECONDS.toMillis(cur - prevTime), TimeUnit.MILLISECONDS)(
            MetricsContext(context.toMap)
          )
        }
      }
    }

    private def processSyncErr(err: com.google.rpc.status.Status): Unit =
      // remove from pending
      removePending(key).foreach { cur =>
        if (config.maxFailed.value > 0) {
          updateWithStatus(err, CommandState.COMMAND_STATE_FAILED)
          addToCollection(cur.status.get(), failed, config.maxFailed.value)
        }
      }

    def failedSync(err: StatusRuntimeException): Unit = {
      val tmp =
        com.google.rpc.status.Status.fromJavaProto(io.grpc.protobuf.StatusProto.fromThrowable(err))
      processSyncErr(tmp)
    }
    def internalErrorSync(err: Throwable): Unit =
      processSyncErr(encodeInternalError(err))
    private def encodeInternalError(err: Throwable): com.google.rpc.status.Status =
      com.google.rpc.status.Status
        .of(com.google.rpc.Code.INTERNAL_VALUE, err.getMessage, Seq.empty)

    def failedAsync(
        status: Option[com.google.rpc.status.Status]
    ): Unit =
      // no need to remove from pending as it will be removed separately
      updateWithStatus(
        status.getOrElse(
          encodeInternalError(new IllegalStateException("Missing status upon failed completion"))
        ),
        CommandState.COMMAND_STATE_FAILED,
      )

    def succeeded(): Unit = {
      recordProgress(Phase7_publishing)
      updateWithStatus(CompletionFromTransaction.OkStatus, CommandState.COMMAND_STATE_SUCCEEDED)
    }

    def recordEnvelopeSizes(
        rootHash: RootHash,
        requestSize: Int,
        numRecipients: Int,
        numEnvelopes: Int,
    ): Unit = {
      recordProgress(Phase1b_ViewBuilding)
      pendingByRootHash.put(rootHash.unwrap, this).discard
      status
        .updateAndGet(
          _.copy(
            requestStatistics = RequestStatistics(
              requestSize = requestSize,
              recipients = numRecipients,
              envelopes = numEnvelopes,
            ),
            rootHash = Some(rootHash.unwrap),
          )
        )
        .discard
    }

    def recordTransactionImpact(
        transaction: LfSubmittedTransaction
    ): Unit = {
      recordProgress(Phase1a_Interpretation)
      val creates = mutable.ListBuffer.empty[Contract]
      val archives = mutable.ListBuffer.empty[Contract]
      final case class Stats(
          exercised: Int = 0,
          fetched: Int = 0,
          lookedUpByKey: Int = 0,
      )
      def mk(
          templateId: TypeConId,
          coid: String,
          keyOpt: Option[GlobalKeyWithMaintainers],
      ): Contract =
        Contract(
          templateId = Some(
            Identifier(
              templateId.packageId,
              templateId.qualifiedName.module.toString,
              templateId.qualifiedName.name.toString,
            )
          ),
          contractId = coid,
          contractKey =
            keyOpt.flatMap(x => LfEngineToApi.lfValueToApiValue(verbose = false, x.value).toOption),
        )
      def leaf(leafOnlyAction: LeafOnlyAction, stats: Stats): Stats =
        leafOnlyAction match {
          case c: Node.Create =>
            creates += mk(c.templateId, c.coid.coid, c.keyOpt)
            stats
          case _: Node.Fetch => stats.copy(fetched = stats.fetched + 1)
          case _: Node.LookupByKey => stats.copy(lookedUpByKey = stats.lookedUpByKey + 1)
        }
      val stats = transaction.foldInExecutionOrder(Stats())(
        exerciseBegin = (acc, _, exerciseNode) => {
          if (exerciseNode.consuming) {
            archives += mk(
              exerciseNode.templateId,
              exerciseNode.targetCoid.coid,
              exerciseNode.keyOpt,
            )
          }
          (acc.copy(exercised = acc.exercised + 1), ChildrenRecursion.DoRecurse)
        },
        rollbackBegin = (acc, _, _) => {
          (acc, ChildrenRecursion.DoNotRecurse)
        },
        leaf = (acc, _, leafNode) => leaf(leafNode, acc),
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      status
        .updateAndGet(
          _.copy(
            updates = CommandUpdates(
              created = creates.toList,
              archived = archives.toList,
              exercised = stats.exercised,
              fetched = stats.fetched,
              lookedUpByKey = stats.lookedUpByKey,
            )
          )
        )
        .discard
    }

    override def transactionSequenced(): Unit =
      recordProgress(Phase2_Sequencing)

  }

  // command key is (commandId, userId, actAs, submissionId)
  private type CommandKey = (String, String, Set[String], Option[String])
  private val pending = new TrieMap[CommandKey, MyCommandResultHandle]()
  private val pendingByRootHash = new TrieMap[Hash, MyCommandResultHandle]()
  private val pendingCount = new AtomicInteger(0)
  private val failed = new mutable.ArrayDeque[CommandStatus](config.maxFailed.value)
  private val succeeded = new mutable.ArrayDeque[CommandStatus](config.maxSucceeded.value)
  private val lock = new Mutex()

  private def removePending(key: CommandKey): Option[MyCommandResultHandle] = {
    val ret = pending.remove(key)
    ret match {
      case Some(removed) =>
        pendingCount.decrementAndGet().discard
        removed.status.get().rootHash.foreach { hash =>
          pendingByRootHash.remove(hash).discard
        }
      case None =>
    }
    ret
  }

  private def findCommands(
      commandIdPrefix: String,
      limit: Int,
      collection: => Iterable[CommandStatus],
  ): Seq[CommandStatus] =
    lock.exclusive {
      collection.filter(_.completion.commandId.startsWith(commandIdPrefix)).take(limit).toSeq
    }

  override def findCommandStatus(
      commandIdPrefix: String,
      state: CommandState,
      limit: Int,
  ): Future[Seq[CommandStatus]] = Future {
    val pool = state match {
      case CommandState.COMMAND_STATE_UNSPECIFIED | CommandState.Unrecognized(_) =>
        findCommands(commandIdPrefix, limit, failed) ++
          findCommands(commandIdPrefix, limit, pending.values.map(_.status.get())) ++
          findCommands(commandIdPrefix, limit, succeeded)
      case CommandState.COMMAND_STATE_FAILED => findCommands(commandIdPrefix, limit, failed)
      case CommandState.COMMAND_STATE_PENDING =>
        findCommands(commandIdPrefix, limit, pending.values.map(_.status.get()))
      case CommandState.COMMAND_STATE_SUCCEEDED => findCommands(commandIdPrefix, limit, succeeded)
    }
    pool.take(limit)
  }

  override def registerCommand(
      commandId: String,
      submissionId: Option[String],
      userId: String,
      commands: Seq[Command],
      actAs: Set[String],
  )(implicit traceContext: TraceContext): CommandResultHandle = if (
    pendingCount.get() >= config.maxPending.value
  ) {
    logger.debug("Already too many pending commands. Stopping tracking.")
    CommandResultHandle.NoOp
  } else {
    val key = (commandId, userId, actAs, submissionId)
    logger.debug(s"Registering handle for $key")
    val status = CommandStatus(
      started = clock.now,
      completed = None,
      completion = CompletionFromTransaction.toApiCompletion(
        CommonCompletionProperties(
          submitters = Set.empty,
          commandId = commandId,
          userId = userId,
          submissionId = submissionId,
          completionOffset = 0L,
          synchronizerTime = None,
          traceContext = SerializableTraceContext(traceContext).toDamlProto,
          deduplicationOffset = None,
          deduplicationDurationSeconds = None,
          deduplicationDurationNanos = None,
        ),
        updateId = "",
        optStatus = None,
      ),
      state = CommandState.COMMAND_STATE_PENDING,
      commands = commands,
      requestStatistics = RequestStatistics.defaultInstance,
      updates = CommandUpdates.defaultInstance,
      synchronizerId = None,
      rootHash = None,
      timings = Seq.empty,
    )
    val handle = new MyCommandResultHandle(key, new AtomicReference(status))
    pending.put(key, handle) match {
      case Some(prev) =>
        // in theory, this can happen if an app sends the same command twice, so it's not
        // a warning ...
        logger.info(s"Duplicate command registration for ${prev.status.get()}")
        prev.status.get().rootHash.foreach(pendingByRootHash.remove(_).discard)
      case None => pendingCount.incrementAndGet().discard
    }
    handle
  }

  override def findHandle(
      commandId: String,
      userId: String,
      actAs: Seq[String],
      submissionId: Option[String],
  ): CommandResultHandle =
    lock.exclusive {
      pending.getOrElse(
        (commandId, userId, actAs.toSet, submissionId),
        CommandResultHandle.NoOp,
      )
    }

  private def addToCollection(
      commandStatus: CommandStatus,
      collection: mutable.ArrayDeque[CommandStatus],
      maxSize: Int,
  ): Unit =
    lock.exclusive {
      collection.prepend(commandStatus)
      if (collection.sizeIs > maxSize) {
        collection.removeLast().discard
      }
    }

  override def processLedgerUpdate(update: TransactionLogUpdate): Unit =
    update match {
      case rejected: TransactionLogUpdate.TransactionRejected =>
        rejected.completionStreamResponse.completionResponse.completion.foreach { completionInfo =>
          val key = (
            completionInfo.commandId,
            completionInfo.userId,
            completionInfo.actAs.toSet,
            Option.when(completionInfo.submissionId.nonEmpty)(completionInfo.submissionId),
          )
          // remove from pending
          removePending(key).foreach { cur =>
            if (config.maxFailed.value > 0) {
              cur.failedAsync(completionInfo.status)
              addToCollection(cur.status.get(), failed, config.maxFailed.value)
            }
          }
        }

      case accepted: TransactionLogUpdate.TransactionAccepted =>
        accepted.completionStreamResponse
          .flatMap(_.completionResponse.completion)
          .foreach { completion =>
            val key = (
              completion.commandId,
              completion.userId,
              completion.actAs.toSet,
              Option.when(completion.submissionId.nonEmpty)(completion.submissionId),
            )
            // remove from pending
            removePending(key).foreach { cur =>
              // mark as done
              cur.succeeded()
              addToCollection(cur.status.get(), succeeded, config.maxSucceeded.value)
            }
          }

      case _ =>
    }

  override def validationStarts(rootHash: RootHash): Unit =
    pendingByRootHash.get(rootHash.unwrap).foreach(_.recordProgress(Phase2b_Queueing))
  override def validationCompleted(rootHash: RootHash): Unit =
    pendingByRootHash.get(rootHash.unwrap).foreach(_.recordProgress(Phase3_ConformanceChecking))
  override def validationResponseCompleted(rootHash: RootHash): Unit =
    pendingByRootHash.get(rootHash.unwrap).foreach(_.recordProgress(Phase4_ConflictDetection))
  override def validationVerdict(rootHash: RootHash): Unit =
    pendingByRootHash.get(rootHash.unwrap).foreach(_.recordProgress(Phase56_MediatorVerdict))

}

object CommandProgressTrackerImpl {
  private abstract class OrderedStage(val ord: Int, val description: String)

  private object Phase_Begin extends OrderedStage(ord = 0, "<begin>")

  private object Phase1a_Interpretation extends OrderedStage(ord = 1, "interpretation")

  private object Phase1b_ViewBuilding extends OrderedStage(ord = 2, "request-generation")

  private object Phase2_Sequencing extends OrderedStage(ord = 3, "sequencing")
  private object Phase2b_Queueing extends OrderedStage(ord = 4, "validation-queue")
  private object Phase3_ConformanceChecking extends OrderedStage(ord = 5, "validation-conformance")

  private object Phase4_ConflictDetection extends OrderedStage(ord = 6, "validation-conflict")

  private object Phase56_MediatorVerdict extends OrderedStage(ord = 7, "mediator-verdict")

  private object Phase7_publishing extends OrderedStage(ord = 8, "publishing")

}
