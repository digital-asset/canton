// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox.bridge.validate

import cats.data.EitherT
import cats.syntax.foldable.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Transaction as LfTransaction
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.entries.LoggingEntry
import com.daml.metrics.Timed
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractState,
  IndexService,
  MaximumLedgerTime,
}
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.ledger.sandbox.bridge.BridgeMetrics
import com.digitalasset.canton.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge.*
import com.digitalasset.canton.ledger.sandbox.domain.Rejection
import com.digitalasset.canton.ledger.sandbox.domain.Rejection.*
import com.digitalasset.canton.ledger.sandbox.domain.Submission.Transaction
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Conflict checking for incoming submissions against the ledger state
  * as it is visible on the Ledger API.
  */
private[validate] class ConflictCheckWithCommittedImpl(
    indexService: IndexService,
    bridgeMetrics: BridgeMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ConflictCheckWithCommitted
    with NamedLogging {

  private def toSubmissionId(id: Ref.SubmissionId): LoggingEntry =
    "submissionId" -> id

  override def apply(
      in: Validation[(Offset, PreparedSubmission)]
  ): AsyncValidation[(Offset, PreparedSubmission)] = in match {
    case Left(rejection) => Future.successful(Left(rejection))
    case Right(input @ (_, transactionSubmission: PreparedTransactionSubmission)) =>
      val submissionId = transactionSubmission.submission.submissionId

      LoggingContextWithTrace.withEnrichedLoggingContext(toSubmissionId(submissionId)) {
        implicit loggingContext: LoggingContextWithTrace =>
          withErrorLogger(Some(submissionId)) { implicit errorLoggingContext =>
            Timed
              .future(
                bridgeMetrics.Stages.ConflictCheckWithCommitted.timer,
                validate(transactionSubmission).map(_.map(_ => input)),
              )
          }(loggingContext, logger)
      }(transactionSubmission.submission.loggingContext)

    case Right(validated) => Future.successful(Right(validated))
  }

  private def validate(
      inputSubmission: PreparedTransactionSubmission
  )(implicit
      errorLoggingContext: ContextualizedErrorLogger,
      loggingContext: LoggingContextWithTrace,
  ): Future[Either[Rejection, Unit]] = {
    import inputSubmission.*

    val eitherTF: EitherT[Future, Rejection, Unit] =
      for {
        _ <- validateExplicitDisclosure(
          submission.processedDisclosedContracts,
          submission.submitterInfo.toCompletionInfo(),
        )
        _ <- validateCausalMonotonicity(
          transaction = submission,
          inputContracts = inputContracts,
          transactionLedgerEffectiveTime = submission.transactionMeta.ledgerEffectiveTime,
          divulged = blindingInfo.divulgence.keySet,
        )
        _ <- validateKeyUsages(
          transactionInformees,
          keyInputs,
          submission.loggingContext,
          submission.submitterInfo.toCompletionInfo(),
        )
      } yield ()

    eitherTF.value
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): EitherT[Future, Rejection, Unit] = {
    val referredContracts = inputContracts.diff(divulged)
    val completionInfo = transaction.submitterInfo.toCompletionInfo()

    if (referredContracts.isEmpty)
      EitherT[Future, Rejection, Unit](Future.successful(Right(())))
    else
      EitherT(
        indexService
          .lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(transaction.loggingContext)
          .transform {
            case Success(MaximumLedgerTime.Archived(missingContractIds)) =>
              Success(Left(UnknownContracts(missingContractIds)(completionInfo)))

            case Failure(err) =>
              Success(Left(LedgerBridgeInternalError(err, completionInfo)))

            case Success(MaximumLedgerTime.Max(maximumLedgerEffectiveTime))
                if maximumLedgerEffectiveTime > transactionLedgerEffectiveTime =>
              Success(
                Left(
                  CausalMonotonicityViolation(
                    contractLedgerEffectiveTime = maximumLedgerEffectiveTime,
                    transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                  )(completionInfo)
                )
              )

            case Success(_) => Success(Right(()))
          }
      )
  }

  private def validateKeyUsages(
      transactionInformees: Set[Ref.Party],
      keyInputs: KeyInputs,
      loggingContext: LoggingContextWithTrace,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): EitherT[Future, Rejection, Unit] =
    keyInputs.toList.collectFirstSomeM { case (key, inputState) =>
      indexService
        .lookupContractKey(transactionInformees, key)(loggingContext)
        .map { lookupResult =>
          (inputState, lookupResult) match {
            case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
              Left(InconsistentContractKey(None, Some(actual))(completionInfo))
            case (LfTransaction.KeyCreate, Some(_)) =>
              Left(DuplicateKey(key)(completionInfo))
            case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
              Left(InconsistentContractKey(Some(expected), actual)(completionInfo))
            case _ => Right(())
          }
        }
        .map(_.left.toOption)
    }.invertToEitherT

  private def validateExplicitDisclosure(
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
      completionInfo: CompletionInfo,
  )(implicit
      errorLoggingContext: ContextualizedErrorLogger,
      loggingContext: LoggingContextWithTrace,
  ): EitherT[Future, Rejection, Unit] =
    // Validation fails fast on the first unknown/invalid contract.
    processedDisclosedContracts.toList.collectFirstSomeM { processedDisclosedContract =>
      val validation = for {
        _ <- validateDisclosedContractPayload(processedDisclosedContract, completionInfo)
        _ <- validateDriverMetadataContractId(completionInfo, processedDisclosedContract)
      } yield ()

      validation.value.map(_.left.toOption)
    }.invertToEitherT

  private def validateDisclosedContractPayload(
      provided: ProcessedDisclosedContract,
      completionInfo: CompletionInfo,
  )(implicit
      errorLoggingContext: ContextualizedErrorLogger,
      loggingContext: LoggingContextWithTrace,
  ): EitherT[Future, Rejection, Unit] =
    EitherT(
      indexService
        .lookupContractStateWithoutDivulgence(provided.contractId)
        .map {
          case ContractState.Archived | ContractState.NotFound =>
            // Disclosed contract was archived or never existed
            Left(UnknownContracts(Set(provided.contractId))(completionInfo))
          case ContractState.Active(contractInstance, ledgerEffectiveTime) =>
            sameContractData(contractInstance, ledgerEffectiveTime, provided).left.map {
              errMessage =>
                logger.info(errMessage)
                DisclosedContractInvalid(provided.contractId, completionInfo)
            }
        }
    )

  private def validateDriverMetadataContractId(
      completionInfo: CompletionInfo,
      provided: ProcessedDisclosedContract,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): EitherT[Future, Rejection, Unit] =
    EitherT
      .fromEither[Future](
        ContractId.V1
          .fromString(provided.driverMetadata.toHexString)
      )
      .leftMap(_ => Rejection.DisclosedContractInvalid(provided.contractId, completionInfo))
      .subflatMap(decodedCid =>
        Either.cond(
          decodedCid == provided.contractId,
          (),
          Rejection
            .DisclosedContractInvalid(provided.contractId, completionInfo),
        )
      )

  private def sameContractData(
      actualContractInstance: Value.VersionedContractInstance,
      actualLedgerEffectiveTime: Timestamp,
      provided: ProcessedDisclosedContract,
  ): Either[String, Unit] = {
    val providedContractId = provided.contractId

    val actualTemplate = actualContractInstance.unversioned.template
    val providedTemplate = provided.templateId

    val providedContractInstance = provided.create.versionedCoinst

    val providedLet = provided.createdAt

    if (actualTemplate != providedTemplate)
      Left(s"Disclosed contract $providedContractId has invalid template id")
    else if (actualContractInstance != providedContractInstance)
      Left(s"Disclosed contract $providedContractId has invalid argument")
    else if (actualLedgerEffectiveTime != providedLet)
      Left(s"Disclosed contract $providedContractId has invalid ledgerEffectiveTime")
    else
      Right(())
  }

  private implicit class FutureValidationOps(val e: Future[Option[Rejection]]) {
    def invertToEitherT: EitherT[Future, Rejection, Unit] = EitherT.fromOptionF(e, ()).swap
  }
}
