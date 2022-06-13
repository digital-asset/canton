// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.lf.CantonOnly
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine._
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.Ast.Package
import com.daml.lf.transaction.Versioned
import com.digitalasset.canton.{LfCommand, LfCreateCommand, LfKeyResolver, LfPartyId, LfVersioned}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.store.ContractAndKeyLookup
import com.digitalasset.canton.participant.util.DAMLe.{ContractWithMetadata, PackageResolver}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LfTransactionUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DAMLe {

  /** Resolves packages by [[com.daml.lf.data.Ref.PackageId]].
    * The returned packages must have been validated
    * so that [[com.daml.lf.engine.Engine]] can skip validation.
    */
  type PackageResolver = PackageId => TraceContext => Future[Option[Package]]

  case class ContractWithMetadata(
      instance: LfContractInst,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      templateId: LfTemplateId,
      keyWithMaintainers: Option[LfKeyWithMaintainers],
  ) {
    def metadataWithGlobalKey: Either[Error, ContractMetadata] = {
      keyWithMaintainers
        .traverse(
          // contract keys are not allowed to contain contract id:
          kwm =>
            LfTransactionUtil
              .globalKeyWithMaintainers(templateId, kwm)
              .leftMap(_ =>
                Error.Interpretation(
                  Error.Interpretation
                    .DamlException(LfInterpretationError.ContractIdInContractKey(kwm.key)),
                  detailMessage =
                    None, // no detail to add to interpretation error generated outside of lf
                )
              )
        )
        .map(safeKeyWithMaintainers =>
          ContractMetadata.tryCreate(
            signatories,
            stakeholders,
            safeKeyWithMaintainers.map(LfVersioned(instance.version, _)),
          )
        )
    }
  }

  val zeroSeed: LfHash = LfHash.assertFromByteArray(new Array[Byte](LfHash.underlyingHashLength))

  // Helper to ensure the package service resolver uses the caller's trace context.
  def packageResolver(
      packageService: PackageService
  ): PackageId => TraceContext => Future[Option[Package]] =
    pkgId => traceContext => packageService.getPackage(pkgId)(traceContext)

}

/** Represents a Daml runtime instance for interpreting commands. Provides an abstraction for the Daml engine
  * handling requests for contract instance lookup as well as in resolving packages.
  * The recommended execution context is to use a work stealing pool.
  *
  * @param resolvePackage A resolver for resolving packages
  * @param ec The execution context where Daml interpretation and validation are execution
  */
class DAMLe(
    resolvePackage: PackageResolver,
    engine: Engine,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  logger.debug(engine.info.show)(TraceContext.empty)

  def reinterpret(
      contracts: ContractAndKeyLookup,
      submitters: Set[LfPartyId],
      command: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      expectFailure: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    Error,
    (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
  ] = {

    def peelAwayRootLevelRollbackNode(
        tx: LfVersionedTransaction
    ): Either[Error, LfVersionedTransaction] =
      if (!expectFailure) {
        Right(tx)
      } else {
        def err(msg: String): Left[Error, LfVersionedTransaction] =
          Left(
            Error.Interpretation(
              Error.Interpretation.Internal("engine.reinterpret", msg, None),
              detailMessage = None,
            )
          )

        tx.roots.get(0).map(nid => nid -> tx.nodes(nid)) match {
          case Some((rootNid, LfNodeRollback(children))) =>
            children.toSeq match {
              case Seq(singleChildNodeId) =>
                tx.nodes(singleChildNodeId) match {
                  case _: LfActionNode =>
                    Right(
                      CantonOnly
                        .lfVersionedTransaction(
                          tx.version,
                          tx.nodes - rootNid,
                          ImmArray(singleChildNodeId),
                        )
                    )
                  case LfNodeRollback(_) =>
                    err(s"Root-level rollback node not expected to parent another rollback node")
                }
              case Seq() => err(s"Root-level rollback node not expected to have no child node")
              case multipleChildNodes =>
                err(
                  s"Root-level rollback node not expect to have more than one child, but found ${multipleChildNodes.length}"
                )
            }
          case nonRollbackNode =>
            err(
              s"Expected failure to be turned into a root rollback node, but encountered ${nonRollbackNode}"
            )
        }
      }

    val result = LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      engine.reinterpret(
        submitters = submitters,
        command = command,
        nodeSeed = rootSeed,
        submissionTime = submissionTime.toLf,
        ledgerEffectiveTime = ledgerTime.toLf,
      )
    }

    for {
      txWithMetadata <- EitherT(handleResult(contracts, result))
      (tx, metadata) = txWithMetadata
      txNoRootRollback <- EitherT.fromEither[Future](peelAwayRootLevelRollbackNode(tx))
    } yield (
      txNoRootRollback,
      TransactionMetadata.fromLf(ledgerTime, metadata),
      metadata.globalKeyMapping,
    )
  }

  def contractWithMetadata(contractInstance: LfContractInst, supersetOfSignatories: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): EitherT[Future, Error, ContractWithMetadata] = {

    val unversionedContractInst = contractInstance.unversioned
    val create = LfCreateCommand(unversionedContractInst.template, unversionedContractInst.arg)

    for {
      transactionWithMetadata <- reinterpret(
        ContractAndKeyLookup.noContracts(logger),
        supersetOfSignatories,
        create,
        CantonTimestamp.Epoch,
        CantonTimestamp.Epoch,
        Some(DAMLe.zeroSeed),
        expectFailure = false,
      )
      (transaction, _metadata, _resolver) = transactionWithMetadata
      md = transaction.nodes(transaction.roots(0)) match {
        case nc @ LfNodeCreate(
              _cid,
              templateId,
              arg,
              agreementText,
              signatories,
              stakeholders,
              key,
              version,
            ) =>
          ContractWithMetadata(
            LfContractInst(templateId, Versioned(version, arg), agreementText),
            signatories,
            stakeholders,
            nc.templateId,
            key,
          )
        case node => throw new RuntimeException(s"DAMLe reinterpreted a create node as $node")
      }
    } yield md
  }

  def contractMetadata(contractInstance: LfContractInst, supersetOfSignatories: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): EitherT[Future, Error, ContractMetadata] =
    for {
      contractAndMetadata <- contractWithMetadata(contractInstance, supersetOfSignatories)
      metadata <- EitherT.fromEither[Future](contractAndMetadata.metadataWithGlobalKey)
    } yield metadata

  private[this] def handleResult[A](contracts: ContractAndKeyLookup, result: Result[A])(implicit
      traceContext: TraceContext
  ): Future[Either[Error, A]] =
    result match {
      case ResultNeedPackage(packageId, resume) =>
        resolvePackage(packageId)(traceContext).transformWith {
          case Success(pkg) =>
            handleResult(contracts, resume(pkg))
          case Failure(ex) =>
            logger.error(s"Package resolution failed for [$packageId]", ex)
            Future.failed(ex)
        }
      case ResultDone(completedResult) => Future.successful(Right(completedResult))
      case ResultNeedKey(key, resume) =>
        val gk = key.globalKey
        contracts
          .lookupKey(gk)
          .toRight(
            Error.Interpretation(
              Error.Interpretation.DamlException(LfInterpretationError.ContractKeyNotFound(gk)),
              None,
            )
          )
          .flatMap(optCid => EitherT(handleResult(contracts, resume(optCid))))
          .value
      case ResultNeedContract(acoid, resume) =>
        contracts
          .lookupLfInstance(acoid)
          .value
          .flatMap(optInst => handleResult(contracts, resume(optInst)))
      case ResultError(err) => Future.successful(Left(err))
    }
}
