// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.{EitherT, NonEmptyList}
import cats.syntax.bifunctor._
import com.daml.lf.engine
import com.digitalasset.canton.data.ViewParticipantData.RootAction
import com.digitalasset.canton.data.{
  CantonTimestamp,
  TransactionView,
  TransactionViewDecomposition,
  TransactionViewTree,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker._
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
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfCommand, LfPartyId, checked}

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
        TraceContext,
    ) => EitherT[Future, DAMLeError, (LfVersionedTransaction, TransactionMetadata)],
    val transactionTreeFactory: TransactionTreeFactory,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Reinterprets the transaction resulting from the received transaction view trees.
    *
    * @param rootViewsWithInputKeys all received transaction view trees contained in a confirmation request that
    *                      have the same transaction id and represent a top-most view
    * @param commonData the common data of all the (rootViewTree :  TransactionViewTree) trees in `rootViewsWithInputKeys`
    * @return the resulting LfTransaction with [[com.digitalasset.canton.protocol.LfContractId]]s only
    */
  def check(
      rootViewsWithInputKeys: NonEmptyList[
        (TransactionViewTree, Map[LfGlobalKey, Option[LfContractId]])
      ],
      requestCounter: RequestCounter,
      topologySnapshot: TopologySnapshot,
      commonData: CommonData,
  )(implicit traceContext: TraceContext): EitherT[Future, Error, Result] = {
    val CommonData(transactionId, ledgerTime, submissionTime, confirmationPolicy) = commonData

    for {
      suffixedTxs <- rootViewsWithInputKeys.traverse { case (v, keys) =>
        checkView(
          v,
          keys,
          requestCounter,
          ledgerTime,
          submissionTime,
          confirmationPolicy,
          topologySnapshot,
        )
      }

      joinedWfTx <- EitherT
        .fromEither[Future](WellFormedTransaction.merge(suffixedTxs))
        .leftMap[Error](err =>
          JoinedTransactionNotWellFormed(suffixedTxs.map(_.unwrap.unwrap).toList, err)
        )
    } yield {
      Result(transactionId, joinedWfTx)
    }
  }

  private def checkView(
      view: TransactionViewTree,
      resolvedKeys: Map[LfGlobalKey, Option[LfContractId]],
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
        resolvedKeys,
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
        traceContext,
      )
        .leftWiden[Error]
      (lfTx, metadata) = lfTxAndMetadata
      wfTx <- EitherT
        .fromEither[Future](
          WellFormedTransaction.normalizeAndCheck(lfTx, metadata, WithoutSuffixes)
        )
        .leftMap[Error](err => TransactionNotWellformed(lfTx, err))
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
        )
      ).leftMap(err =>
        TransactionTreeError(lfTx, s"Failed to construct transaction view tree: $err")
      )
      (reconstructedView, suffixedTx) = reconstructedViewAndTx

      _ <- EitherT.cond[Future](
        view.view == reconstructedView,
        (),
        TransactionTreeError(
          lfTx,
          s"Reconstructed view differs from received view:\nReceived: ${view.view}\nReconstructed: $reconstructedView",
        ): Error,
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
        traceContext: TraceContext,
    ): EitherT[Future, DAMLeError, (LfVersionedTransaction, TransactionMetadata)] =
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
        .leftMap(DAMLeError)

    new ModelConformanceChecker(reinterpret, transactionTreeFactory, loggerFactory)
  }

  sealed trait Error

  /** Indicates that [[ModelConformanceChecker.reinterpret]] has failed. */
  case class DAMLeError(cause: engine.Error) extends Error

  /** Indicates that the received views and the reconstructed view decompositions have a different tree structure. */
  case class ModelConformanceError(
      receivedViews: Seq[TransactionView],
      reconstructedViewDecompositions: Seq[TransactionViewDecomposition],
  ) extends Error

  /** Indicates a different number of declared and reconstructed create nodes. */
  case class CreatedContractsDeclaredIncorrectly(
      declaredCreateNodes: Seq[CreatedContract],
      reconstructedCreateNodes: Seq[LfNodeCreate],
  ) extends Error

  case class TransactionNotWellformed(tx: LfVersionedTransaction, cause: String) extends Error

  case class TransactionTreeError(tx: LfVersionedTransaction, cause: String) extends Error

  case class JoinedTransactionNotWellFormed(tx: Seq[LfVersionedTransaction], cause: String)
      extends Error

  case class Result(
      transactionId: TransactionId,
      suffixedTransaction: WellFormedTransaction[WithSuffixesAndMerged],
  )

}
