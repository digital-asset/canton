// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton._
import com.digitalasset.canton.crypto.{Salt, SaltSeed}
import com.digitalasset.canton.data.{
  GenTransactionTree,
  TransactionView,
  TransactionViewTree,
  ViewPosition,
}
import com.digitalasset.canton.topology.{MediatorId, ParticipantId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.store.ContractLookup
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.tracing.TraceContext

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait TransactionTreeFactory {

  /** Converts a `transaction: LfTransaction` to the corresponding transaction tree, if possible.
    *
    * @see TransactionTreeConversionError for error cases
    */
  def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      confirmationPolicy: ConfirmationPolicy,
      workflowId: Option[WorkflowId],
      mediatorId: MediatorId,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree]

  /** Reconstructs a transaction view from a reinterpreted action description,
    * using the supplied salts.
    *
    * @throws java.lang.IllegalArgumentException if `subaction` does not contain exactly one root node
    */
  def tryReconstruct(
      subaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      confirmationPolicy: ConfirmationPolicy,
      mediatorId: MediatorId,
      salts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      rbContext: RollbackContext,
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    TransactionTreeConversionError,
    (TransactionView, WellFormedTransaction[WithSuffixes]),
  ]

  /** Extracts the salts for the view from a transaction view tree.
    * The salts appear in the same order as they are needed by [[tryReconstruct]].
    */
  def saltsFromView(view: TransactionViewTree): Iterable[Salt]

}

object TransactionTreeFactory {

  type SerializableContractOfId =
    LfContractId => EitherT[Future, ContractLookupError, SerializableContract]
  def contractInstanceLookup(contractStore: ContractLookup)(id: LfContractId)(implicit
      ex: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, ContractLookupError, SerializableContract] =
    contractStore.lookupContract(id).toRight(ContractLookupError(id, "Unknown contract"))

  /** Supertype for all errors than may arise during the conversion. */
  sealed trait TransactionTreeConversionError extends Product with Serializable with PrettyPrinting

  /** Indicates that a contract instance could not be looked up by an instance of [[SerializableContractOfId]]. */
  case class ContractLookupError(id: LfContractId, message: String)
      extends TransactionTreeConversionError {
    override def pretty: Pretty[ContractLookupError] = prettyOfClass(
      param("id", _.id),
      param("message", _.message.unquoted),
    )
  }

  case class SubmitterMetadataError(message: String) extends TransactionTreeConversionError {
    override def pretty: Pretty[SubmitterMetadataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  // TODO(i3013) Remove this error
  case class ViewParticipantDataError(message: String) extends TransactionTreeConversionError {
    override def pretty: Pretty[ViewParticipantDataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  case class DivergingKeyResolutionError(divergingKeys: Map[LfGlobalKey, Set[Option[LfContractId]]])
      extends TransactionTreeConversionError {
    override def pretty: Pretty[DivergingKeyResolutionError] = prettyOfClass(
      unnamedParam(_.divergingKeys)
    )
  }

  /** Indicates that too few salts have been supplied for creating a view */
  case object TooFewSalts extends TransactionTreeConversionError {
    override def pretty: Pretty[TooFewSalts.type] = prettyOfObject[TooFewSalts.type]
  }
  type TooFewSalts = TooFewSalts.type

  case class UnknownPackageError(unknownTo: Seq[PackageUnknownTo])
      extends TransactionTreeConversionError {
    override def pretty: Pretty[UnknownPackageError] =
      prettyOfString(err => show"Some packages are not known to all informees.\n${err.unknownTo}")
  }

  case class PackageUnknownTo(
      packageId: LfPackageId,
      description: String,
      participantId: ParticipantId,
  ) extends PrettyPrinting {
    override def pretty: Pretty[PackageUnknownTo] = prettyOfString { put =>
      show"Participant $participantId has not vetted ${put.description.doubleQuoted} (${put.packageId})"
    }
  }
}
