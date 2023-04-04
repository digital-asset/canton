// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{
  Engine,
  Error as DamlLfError,
  Result,
  ResultDone,
  ResultError,
  ResultInterruption,
  ResultNeedAuthority,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
}
import com.daml.lf.transaction.{Node, SubmittedTransaction, Transaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed, Tracked}
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractStore,
  IndexPackagesService,
}
import com.digitalasset.canton.ledger.participant.state.{v2 as state}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import scalaz.syntax.tag.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

/** @param ec [[scala.concurrent.ExecutionContext]] that will be used for scheduling CPU-intensive computations
  *           performed by an [[com.daml.lf.engine.Engine]].
  */
private[apiserver] final class StoreBackedCommandExecutor(
    engine: Engine,
    participant: Ref.ParticipantId,
    packagesService: IndexPackagesService,
    contractStore: ContractStore,
    authorityResolver: AuthorityResolver,
    metrics: Metrics,
)(implicit
    ec: ExecutionContext
) extends CommandExecutor {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[this] val packageLoader = new DeduplicatingPackageLoader()

  override def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    val interpretationTimeNanos = new AtomicLong(0L)
    val start = System.nanoTime()
    for {
      submissionResult <- submitToEngine(commands, submissionSeed, interpretationTimeNanos)
      submission <- consume(
        commands.actAs,
        commands.readAs,
        submissionResult,
        interpretationTimeNanos,
      )
    } yield {
      submission
        .map { case (updateTx, meta) =>
          val interpretationTimeNanos = System.nanoTime() - start
          commandExecutionResult(
            commands,
            submissionSeed,
            ledgerConfiguration,
            updateTx,
            meta,
            interpretationTimeNanos,
          )
        }
        .left
        .map(ErrorCause.DamlLf)
    }
  }

  private def commandExecutionResult(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
      updateTx: SubmittedTransaction,
      meta: Transaction.Metadata,
      interpretationTimeNanos: Long,
  ) = {
    CommandExecutionResult(
      submitterInfo = state.SubmitterInfo(
        commands.actAs.toList,
        commands.readAs.toList,
        commands.applicationId,
        commands.commandId.unwrap,
        commands.deduplicationPeriod,
        commands.submissionId.map(_.unwrap),
        ledgerConfiguration,
      ),
      transactionMeta = state.TransactionMeta(
        commands.commands.ledgerEffectiveTime,
        commands.workflowId.map(_.unwrap),
        meta.submissionTime,
        submissionSeed,
        Some(meta.usedPackages),
        Some(meta.nodeSeeds),
        Some(
          updateTx.nodes
            .collect { case (nodeId, node: Node.Action) if node.byKey => nodeId }
            .to(ImmArray)
        ),
      ),
      transaction = updateTx,
      dependsOnLedgerTime = meta.dependsOnTime,
      interpretationTimeNanos = interpretationTimeNanos,
      globalKeyMapping = meta.globalKeyMapping,
      processedDisclosedContracts = meta.processedDisclosedContracts,
    )
  }

  private def submitToEngine(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      interpretationTimeNanos: AtomicLong,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[(SubmittedTransaction, Transaction.Metadata)]] =
    Tracked.future(
      metrics.daml.execution.engineRunning,
      Future(trackSyncExecution(interpretationTimeNanos) {
        // The actAs and readAs parties are used for two kinds of checks by the ledger API server:
        // When looking up contracts during command interpretation, the engine should only see contracts
        // that are visible to at least one of the actAs or readAs parties. This visibility check is not part of the
        // Daml ledger model.
        // When checking Daml authorization rules, the engine verifies that the actAs parties are sufficient to
        // authorize the resulting transaction.
        val commitAuthorizers = commands.actAs
        engine.submit(
          commitAuthorizers,
          commands.readAs,
          commands.commands,
          commands.disclosedContracts,
          participant,
          submissionSeed,
        )
      }),
    )

  private def consume[A](
      actAs: Set[Ref.Party],
      readAs: Set[Ref.Party],
      result: Result[A],
      interpretationTimeNanos: AtomicLong,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[DamlLfError, A]] = {
    val readers = actAs ++ readAs

    val lookupActiveContractTime = new AtomicLong(0L)
    val lookupActiveContractCount = new AtomicLong(0L)

    val lookupContractKeyTime = new AtomicLong(0L)
    val lookupContractKeyCount = new AtomicLong(0L)

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] =
      result match {
        case ResultDone(r) => Future.successful(Right(r))

        case ResultError(err) => Future.successful(Left(err))

        case ResultNeedContract(acoid, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupActiveContract,
              contractStore.lookupActiveContract(readers, acoid),
            )
            .flatMap { instance =>
              lookupActiveContractTime.addAndGet(System.nanoTime() - start)
              lookupActiveContractCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(instance)),
                )
              )
            }

        case ResultNeedKey(key, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupContractKey,
              contractStore.lookupContractKey(readers, key.globalKey),
            )
            .flatMap { contractId =>
              lookupContractKeyTime.addAndGet(System.nanoTime() - start)
              lookupContractKeyCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(contractId)),
                )
              )
            }

        case ResultNeedPackage(packageId, resume) =>
          packageLoader
            .loadPackage(
              packageId = packageId,
              delegate = packageId => packagesService.getLfArchive(packageId)(loggingContext),
              metric = metrics.daml.execution.getLfPackage,
            )
            .flatMap { maybePackage =>
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(maybePackage)),
                )
              )
            }

        case ResultInterruption(continue) =>
          resolveStep(
            Tracked.value(
              metrics.daml.execution.engineRunning,
              trackSyncExecution(interpretationTimeNanos)(continue()),
            )
          )

        case ResultNeedAuthority(holding @ _, requesting @ _, resume) =>
          authorityResolver
            // TODO(i11255) DomainId is required to be passed here
            .resolve(AuthorityResolver.AuthorityRequest(holding, requesting, domainId = None))
            .flatMap { response =>
              val resumed = response match {
                case AuthorityResolver.AuthorityResponse.MissingAuthorisation(parties) =>
                  val receivedAuthorityFor = (parties -- requesting).mkString(",")
                  val missingAuthority = parties.mkString(",")
                  logger.debug(
                    s"Authorisation failed. Missing authority: [$missingAuthority]. Received authority for: [$receivedAuthorityFor]"
                  )
                  false
                case AuthorityResolver.AuthorityResponse.Authorized =>
                  true
              }
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(resumed)),
                )
              )
            }
      }

    resolveStep(result).andThen { case _ =>
      metrics.daml.execution.lookupActiveContractPerExecution
        .update(lookupActiveContractTime.get(), TimeUnit.NANOSECONDS)
      metrics.daml.execution.lookupActiveContractCountPerExecution
        .update(lookupActiveContractCount.get)
      metrics.daml.execution.lookupContractKeyPerExecution
        .update(lookupContractKeyTime.get(), TimeUnit.NANOSECONDS)
      metrics.daml.execution.lookupContractKeyCountPerExecution
        .update(lookupContractKeyCount.get())
      metrics.daml.execution.engine
        .update(interpretationTimeNanos.get(), TimeUnit.NANOSECONDS)
    }
  }

  private def trackSyncExecution[T](atomicNano: AtomicLong)(computation: => T): T = {
    val start = System.nanoTime()
    val result = computation
    atomicNano.addAndGet(System.nanoTime() - start)
    result
  }
}
