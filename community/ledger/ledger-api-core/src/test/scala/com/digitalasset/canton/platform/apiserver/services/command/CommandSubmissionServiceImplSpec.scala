// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.{DeduplicationPeriod, LedgerTimeBoundaries}
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{CommandId, Commands, DisclosedContract}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{
  RoutingSynchronizerState,
  SubmissionResult,
  SubmitterInfo,
  SynchronizerRank,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandExecutionResult,
  CommandExecutor,
  CommandInterpretationResult,
}
import com.digitalasset.canton.platform.apiserver.services.{ErrorCause, TimeProviderType}
import com.digitalasset.canton.platform.apiserver.{FatContractInstanceHelper, SeedService}
import com.digitalasset.canton.protocol.LfTransactionVersion
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.command.ApiCommands as LfCommands
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageName}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.daml.lf.language.{LookupError, Reference}
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.*
import com.digitalasset.daml.lf.transaction.test.{
  TestNodeBuilder,
  TransactionBuilder,
  TreeTransactionBuilder,
}
import com.digitalasset.daml.lf.transaction.{Node as _, *}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status as RpcStatus
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}
import java.util.concurrent.CompletableFuture
import scala.util.{Failure, Success, Try}

class CommandSubmissionServiceImplSpec
    extends AnyFlatSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ScalaFutures
    with ArgumentMatchersSugar
    with BaseTest
    with HasExecutionContext {

  import TransactionBuilder.Implicits.*

  private implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val knownParties = (1 to 100).map(idx => s"party-$idx").toArray
  private val missingParties = (101 to 200).map(idx => s"party-$idx").toArray
  private val allInformeesInTransaction = knownParties ++ missingParties

  private val nodes: Seq[NodeWrapper] = for {
    i <- 0 until 100
  } yield {
    // Ensure 100 % overlap by having 4 informees per each of the 100 nodes
    val informeesOfNode = allInformeesInTransaction.slice(i * 4, (i + 1) * 4)
    val (signatories, observers) = informeesOfNode.splitAt(2)
    TestNodeBuilder.create(
      id = Value.ContractId.V1(Hash.hashPrivateKey(i.toString)).coid,
      templateId = "test:test",
      argument = Value.ValueNil,
      signatories = signatories.toSeq,
      observers = observers.toSeq,
    )
  }

  private val transaction = SubmittedTransaction(
    TreeTransactionBuilder.toVersionedTransaction(nodes*)
  )

  behavior of "submit"

  it should "finish successfully in the happy flow" in new TestContext {
    apiSubmissionService()
      .submit(SubmitRequest(commands))(
        LoggingContextWithTrace(TraceContext.empty)
      )
      .futureValueUS
  }

  behavior of "submit"

  it should "return proper gRPC status codes for DamlLf errors" in new TestContext {
    loggerFactory.assertLogs(
      within = {
        val tmplId = toIdentifier("M:T")

        val errorsToExpectedStatuses: Seq[(ErrorCause, Status)] = List(
          ErrorCause.DamlLf(
            LfError.Interpretation(
              LfError.Interpretation.DamlException(
                LfInterpretationError.ContractNotFound("00" + "00" * 32)
              ),
              None,
            )
          ) -> Status.NOT_FOUND,
          ErrorCause.DamlLf(
            LfError.Interpretation(
              LfError.Interpretation.DamlException(
                LfInterpretationError.DuplicateContractKey(
                  GlobalKey
                    .assertBuild(tmplId, Value.ValueUnit, PackageName.assertFromString("pkg-name"))
                )
              ),
              None,
            )
          ) -> Status.ALREADY_EXISTS,
          ErrorCause.DamlLf(
            LfError.Validation(
              LfError.Validation.ReplayMismatch(ReplayMismatch(null, null))
            )
          ) -> Status.INTERNAL,
          ErrorCause.DamlLf(
            LfError.Preprocessing(
              LfError.Preprocessing.Lookup(
                LookupError.NotFound(
                  Reference.Package(defaultPackageId),
                  Reference.Package(defaultPackageId),
                )
              )
            )
          ) -> Status.INVALID_ARGUMENT,
          ErrorCause.DamlLf(
            LfError.Interpretation(
              LfError.Interpretation.DamlException(
                LfInterpretationError.FailedAuthorization(
                  NodeId(1),
                  lf.ledger.FailedAuthorization.NoSignatories(tmplId, None),
                )
              ),
              None,
            )
          ) -> Status.INVALID_ARGUMENT,
          ErrorCause.LedgerTime(0) -> Status.ABORTED,
        )

        // when
        val results = errorsToExpectedStatuses
          .map { case (error, expectedStatus) =>
            when(
              commandExecutor.execute(
                eqTo(commands),
                any[Hash],
                eqTo(routingSynchronizerState),
                anyBoolean,
              )(any[LoggingContextWithTrace])
            ).thenReturn(
              EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult](
                FutureUnlessShutdown.pure(Left(error))
              )
            )

            apiSubmissionService()
              .submit(SubmitRequest(commands))
              .transform(result => Success(UnlessShutdown.Outcome(expectedStatus -> result)))
              .futureValueUS
          }

        // then
        results.foreach { case (expectedStatus: Status, result: Try[UnlessShutdown[Unit]]) =>
          inside(result) { case Failure(exception) =>
            exception.getMessage should startWith(expectedStatus.getCode.toString)
          }
        }
      },
      assertions = _.errorMessage should include(
        "LEDGER_API_INTERNAL_ERROR(4,0): Observed un-expected replay mismatch"
      ),
      _.errorMessage should include("Unhandled internal error"),
    )
  }

  it should "rate-limit when configured to do so" in new TestContext {
    val grpcError = RpcStatus.of(Status.Code.ABORTED.value(), s"Quota Exceeded", Seq.empty)

    apiSubmissionService(checkOverloaded = _ => Some(SubmissionResult.SynchronousError(grpcError)))
      .submit(SubmitRequest(commands))
      .transform {
        case Failure(e: StatusRuntimeException)
            if e.getStatus.getCode.value == grpcError.code && e.getStatus.getDescription == grpcError.message =>
          Success(UnlessShutdown.Outcome(succeed))
        case result =>
          fail(s"Expected submission to be aborted, but got $result")
      }
      .futureValueUS
  }

  private trait TestContext {
    val syncService = mock[state.SyncService]
    val timeProvider = TimeProvider.Constant(Instant.now)
    val timeProviderType = TimeProviderType.Static
    val seedService = SeedService.WeakRandom
    val commandExecutor = mock[CommandExecutor]
    val metrics = LedgerApiServerMetrics.ForTesting
    val alice = Ref.Party.assertFromString("alice")

    val synchronizerId: SynchronizerId = SynchronizerId.tryFromString("x::synchronizerId")

    val processedDisclosedContract =
      FatContractInstanceHelper.buildFatContractInstance(
        templateId = Identifier.assertFromString("some:pkg:identifier"),
        packageName = PackageName.assertFromString("pkg-name"),
        contractId = TransactionBuilder.newCid,
        argument = Value.ValueNil,
        createdAt = Timestamp.Epoch,
        driverMetadata = Bytes.Empty,
        signatories = Set(alice),
        stakeholders = Set(alice),
        keyOpt = None,
        version = LfTransactionVersion.minVersion,
      )

    val disclosedContract = DisclosedContract(
      fatContractInstance = processedDisclosedContract,
      synchronizerIdO = Some(synchronizerId),
    )

    val commands = Commands(
      workflowId = None,
      userId = Ref.UserId.assertFromString("app"),
      commandId = CommandId(Ref.CommandId.assertFromString("cmd")),
      submissionId = None,
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = Timestamp.Epoch,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      commands = LfCommands(
        commands = ImmArray.empty,
        ledgerEffectiveTime = Timestamp.Epoch,
        commandsReference = "",
      ),
      disclosedContracts = ImmArray(disclosedContract),
      synchronizerId = None,
      prefetchKeys = Seq.empty,
    )

    val submitterInfo = SubmitterInfo(
      actAs = Nil,
      readAs = Nil,
      userId = Ref.UserId.assertFromString("foobar"),
      commandId = Ref.CommandId.assertFromString("foobar"),
      deduplicationPeriod = DeduplicationDuration(Duration.ofMinutes(1)),
      submissionId = None,
      externallySignedSubmission = None,
    )
    val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = Timestamp.Epoch,
      workflowId = None,
      preparationTime = Time.Timestamp.Epoch,
      submissionSeed = Hash.hashPrivateKey("SomeHash"),
      timeBoundaries = LedgerTimeBoundaries.unconstrained,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )
    val estimatedInterpretationCost = 5L
    val processedDisclosedContracts = ImmArray(processedDisclosedContract)
    val commandInterpretationResult = CommandInterpretationResult(
      submitterInfo = submitterInfo,
      optSynchronizerId = None,
      transactionMeta = transactionMeta,
      transaction = transaction,
      dependsOnLedgerTime = false,
      interpretationTimeNanos = estimatedInterpretationCost,
      globalKeyMapping = Map.empty,
      processedDisclosedContracts = processedDisclosedContracts,
    )
    val synchronizerRank =
      SynchronizerRank.single(SynchronizerId.tryFromString("da::test").toPhysical)
    val routingSynchronizerState = mock[RoutingSynchronizerState]
    val commandExecutionResult = CommandExecutionResult(
      commandInterpretationResult = commandInterpretationResult,
      synchronizerRank = synchronizerRank,
      routingSynchronizerState = routingSynchronizerState,
    )

    when(syncService.getRoutingSynchronizerState(traceContext)).thenReturn(routingSynchronizerState)

    when(
      commandExecutor.execute(
        eqTo(commands),
        any[Hash],
        eqTo(routingSynchronizerState),
        anyBoolean,
      )(
        any[LoggingContextWithTrace]
      )
    )
      .thenReturn(
        EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult](
          FutureUnlessShutdown.pure(Right(commandExecutionResult))
        )
      )
    when(
      syncService.submitTransaction(
        eqTo(transaction),
        eqTo(synchronizerRank),
        eqTo(routingSynchronizerState),
        eqTo(submitterInfo),
        eqTo(transactionMeta),
        eqTo(estimatedInterpretationCost),
        eqTo(Map.empty),
        eqTo(processedDisclosedContracts),
      )(any[TraceContext])
    ).thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))

    def apiSubmissionService(
        checkOverloaded: TraceContext => Option[state.SubmissionResult] = _ => None
    ) = new CommandSubmissionServiceImpl(
      syncService = syncService,
      timeProviderType = timeProviderType,
      timeProvider = timeProvider,
      seedService = seedService,
      commandExecutor = commandExecutor,
      checkOverloaded = checkOverloaded,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )
  }
}
