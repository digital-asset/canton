// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.lf.engine
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.data.ViewParticipantData.RootAction
import com.digitalasset.canton.data.{CantonTimestamp, TransactionView, TransactionViewTree}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.TransactionTreeConversionError
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.store.{
  ContractAndKeyLookup,
  ContractLookup,
  ExtendedContractLookup,
  StoredContract,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixes,
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{LfCommand, LfKeyResolver, LfPartyId, RequestCounter, checked}

import scala.concurrent.{ExecutionContext, Future}

/** Allows for checking model conformance of a list of transaction view trees.
  * If successful, outputs the received transaction as LfVersionedTransaction along with TransactionMetadata.
  *
  * @param reinterpret reinterprets the lf command to a transaction.
  * @param transactionTreeFactory reconstructs a transaction view from the reinterpreted action description.
  */
class ModelConformanceChecker(
    val reinterpret: (
        ContractAndKeyLookup,
        Set[LfPartyId],
        LfCommand,
        CantonTimestamp,
        CantonTimestamp,
        Option[LfHash],
        Boolean,
        ViewHash,
        TraceContext,
    ) => EitherT[
      Future,
      DAMLeError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
    ],
    val transactionTreeFactory: TransactionTreeFactory,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Reinterprets the transaction resulting from the received transaction view trees.
    *
    * @param rootViews all received transaction view trees contained in a confirmation request that
    *                  have the same transaction id and represent a top-most view
    * @param keyResolverFor The key resolver to be used for re-interpreting root views
    * @param commonData the common data of all the (rootViewTree :  TransactionViewTree) trees in `rootViewsWithInputKeys`
    * @return the resulting LfTransaction with [[com.digitalasset.canton.protocol.LfContractId]]s only
    */
  def check(
      rootViews: NonEmpty[Seq[TransactionViewTree]],
      keyResolverFor: TransactionViewTree => LfKeyResolver,
      requestCounter: RequestCounter,
      topologySnapshot: TopologySnapshot,
      commonData: CommonData,
  )(implicit traceContext: TraceContext): EitherT[Future, Error, Result] = {
    val CommonData(transactionId, ledgerTime, submissionTime, confirmationPolicy) = commonData

    for {
      suffixedTxs <- rootViews.toNEF.parTraverse { v =>
        checkView(
          v,
          keyResolverFor(v),
          requestCounter,
          ledgerTime,
          submissionTime,
          confirmationPolicy,
          topologySnapshot,
        )
      }

      joinedWfTx <- EitherT
        .fromEither[Future](WellFormedTransaction.merge(suffixedTxs))
        .leftMap[Error](JoinedTransactionNotWellFormed)
    } yield {
      Result(transactionId, joinedWfTx)
    }
  }

  private def checkView(
      view: TransactionViewTree,
      resolverFromView: LfKeyResolver,
      requestCounter: RequestCounter,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, WithRollbackScope[WellFormedTransaction[WithSuffixes]]] = {
    val RootAction(cmd, authorizers, failed) = view.viewParticipantData.rootAction
    val rbContext = view.viewParticipantData.rollbackContext
    val seed = view.viewParticipantData.actionDescription.seedOption
    val viewInputContracts = view.flatten
      .flatMap(_.viewParticipantData.coreInputs.map {
        case (cid, InputContract(contract, _consumed)) =>
          (cid, StoredContract(contract, requestCounter, None))
      })
      .toMap
    val lookupWithKeys =
      new ExtendedContractLookup(
        ContractLookup.noContracts(logger), // all contracts and keys specified explicitly
        viewInputContracts,
        resolverFromView,
      )

    for {
      lfTxAndMetadata <- reinterpret(
        lookupWithKeys,
        authorizers,
        cmd,
        ledgerTime,
        submissionTime,
        seed,
        failed,
        view.viewHash,
        traceContext,
      )
        .leftWiden[Error]
      (lfTx, metadata, resolverFromReinterpretation) = lfTxAndMetadata
      // For transaction views of protocol version 3 or higher,
      // the `resolverFromReinterpretation` is the same as the `resolverFromView`.
      // The `TransactionTreeFactoryImplV3` rebuilds the `resolverFromReinterpreation`
      // again by re-running the `ContractStateMachine` and checks consistency
      // with the reconstructed view's global key inputs,
      // which by the view equality check is the same as the `resolverFromView`.
      wfTx <- EitherT
        .fromEither[Future](
          WellFormedTransaction.normalizeAndCheck(lfTx, metadata, WithoutSuffixes)
        )
        .leftMap[Error](err => TransactionNotWellformed(err, view.viewHash))
      salts = transactionTreeFactory.saltsFromView(view)
      reconstructedViewAndTx <- checked(
        transactionTreeFactory.tryReconstruct(
          subaction = wfTx,
          rootPosition = view.viewPosition,
          rbContext = rbContext,
          confirmationPolicy = confirmationPolicy,
          mediatorId = view.mediatorId,
          salts = salts,
          transactionUuid = view.transactionUuid,
          topologySnapshot = topologySnapshot,
          contractOfId = TransactionTreeFactory.contractInstanceLookup(lookupWithKeys),
          keyResolver = resolverFromReinterpretation,
        )
      ).leftMap(err => TransactionTreeError(err, view.viewHash))
      (reconstructedView, suffixedTx) = reconstructedViewAndTx

      _ <- EitherT.cond[Future](
        view.view == reconstructedView,
        (),
        ViewReconstructionError(view.view, reconstructedView): Error,
      )

    } yield WithRollbackScope(rbContext.rollbackScope, suffixedTx)
  }

}

object ModelConformanceChecker {
  def apply(
      damle: DAMLe,
      transactionTreeFactory: TransactionTreeFactory,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ModelConformanceChecker = {
    def reinterpret(
        contracts: ContractAndKeyLookup,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        submissionTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        expectFailure: Boolean,
        viewHash: ViewHash,
        traceContext: TraceContext,
    ): EitherT[Future, DAMLeError, (LfVersionedTransaction, TransactionMetadata, LfKeyResolver)] =
      damle
        .reinterpret(
          contracts,
          submitters,
          command,
          ledgerTime,
          submissionTime,
          rootSeed,
          expectFailure,
        )(traceContext)
        .leftMap(DAMLeError(_, viewHash))

    new ModelConformanceChecker(reinterpret, transactionTreeFactory, loggerFactory)
  }

  sealed trait Error extends PrettyPrinting

  /** Indicates that [[ModelConformanceChecker.reinterpret]] has failed. */
  case class DAMLeError(cause: engine.Error, viewHash: ViewHash) extends Error {
    override def pretty: Pretty[DAMLeError] = adHocPrettyInstance
  }

  /** Indicates a different number of declared and reconstructed create nodes. */
  case class CreatedContractsDeclaredIncorrectly(
      declaredCreateNodes: Seq[CreatedContract],
      reconstructedCreateNodes: Seq[LfNodeCreate],
      viewHash: ViewHash,
  ) extends Error {
    override def pretty: Pretty[CreatedContractsDeclaredIncorrectly] = prettyOfClass(
      param("declaredCreateNodes", _.declaredCreateNodes),
      param(
        "reconstructedCreateNodes",
        _.reconstructedCreateNodes.map(_.templateId),
      ),
      unnamedParam(_.viewHash),
    )
  }

  case class TransactionNotWellformed(cause: String, viewHash: ViewHash) extends Error {
    override def pretty: Pretty[TransactionNotWellformed] = prettyOfClass(
      param("cause", _.cause.unquoted),
      unnamedParam(_.viewHash),
    )
  }

  case class TransactionTreeError(
      details: TransactionTreeConversionError,
      viewHash: ViewHash,
  ) extends Error {

    def cause: String = "Failed to construct transaction tree."

    override def pretty: Pretty[TransactionTreeError] = prettyOfClass(
      param("cause", _.cause.unquoted),
      unnamedParam(_.details),
      unnamedParam(_.viewHash),
    )
  }

  case class ViewReconstructionError(
      received: TransactionView,
      reconstructed: TransactionView,
  ) extends Error {

    def cause = "Reconstructed view differs from received view."

    override def pretty: Pretty[ViewReconstructionError] = prettyOfClass(
      param("cause", _.cause.unquoted),
      param("received", _.received),
      param("reconstructed", _.reconstructed),
    )
  }

  case class JoinedTransactionNotWellFormed(
      cause: String
  ) extends Error {
    override def pretty: Pretty[JoinedTransactionNotWellFormed] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }

  case class Result(
      transactionId: TransactionId,
      suffixedTransaction: WellFormedTransaction[WithSuffixesAndMerged],
  )

}
