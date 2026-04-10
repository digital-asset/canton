// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  GenTransactionTree,
  TransactionView,
  ViewPosition,
}
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractInstanceOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.store.ContractLookup
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithoutSuffixes,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractHasher
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.transaction.{LegacyTransactionErrors, NodeId, TransactionError}

import java.util.UUID
import scala.concurrent.ExecutionContext

trait TransactionTreeFactory {

  /** The [[com.digitalasset.canton.protocol.CantonContractIdVersion]] to be used for newly created
    * contracts
    */
  def cantonContractIdVersion: CantonContractIdVersion

  /** Converts a `transaction: LfTransaction` to the corresponding transaction tree, if possible.
    *
    * @param keyResolver
    *   The key resolutions recorded while interpreting the transaction.
    * @see
    *   TransactionTreeConversionError for error cases
    */
  def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      mediator: MediatorGroupRecipient,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: ContractInstanceOfId,
      keyResolver: LfKeyResolver,
      maxSequencingTime: CantonTimestamp,
      validatePackageVettings: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree]

  /** Reconstructs a transaction view from a reinterpreted action description, using the supplied
    * salts.
    *
    * @param keyResolver
    *   The key resolutions recorded while re-interpreting the subaction.
    * @throws java.lang.IllegalArgumentException
    *   if `subaction` does not contain exactly one root node
    */
  def tryReconstruct(
      subaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
      submittingParticipantO: Option[ParticipantId],
      salts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: ContractInstanceOfId,
      rbContext: RollbackContext,
      keyResolver: LfKeyResolver,
      absolutizer: ContractIdAbsolutizer,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    TransactionTreeConversionError,
    (TransactionView, WellFormedTransaction[WithAbsoluteSuffixes]),
  ]

  /** Extracts the salts for the view from a transaction view tree. The salts appear in the same
    * order as they are needed by [[tryReconstruct]].
    */
  def saltsFromView(view: TransactionView): Iterable[Salt]

}

object TransactionTreeFactory {

  type ContractInstanceOfId =
    LfContractId => EitherT[FutureUnlessShutdown, ContractLookupError, GenContractInstance]

  def apply(
      submittingParticipant: ParticipantId,
      synchronizerId: PhysicalSynchronizerId,
      cantonContractIdVersion: CantonContractIdVersion,
      cryptoOps: HashOps & HmacOps,
      hasher: ContractHasher,
      loggerFactory: NamedLoggerFactory,
  )(implicit ex: ExecutionContext): TransactionTreeFactory =
    if (synchronizerId.protocolVersion >= ProtocolVersion.v35) {
      new NextGenTransactionTreeFactory(
        submittingParticipant,
        synchronizerId,
        cantonContractIdVersion,
        cryptoOps,
        hasher,
        loggerFactory,
      )
    } else {
      new LegacyTransactionTreeFactory(
        submittingParticipant,
        synchronizerId,
        cantonContractIdVersion,
        cryptoOps,
        hasher,
        loggerFactory,
      )
    }

  def contractInstanceLookup(
      contractStore: ContractLookup
  )(implicit ex: ExecutionContext, traceContext: TraceContext): ContractInstanceOfId =
    id =>
      for {
        contract <- contractStore
          .lookup(id)
          .toRight(ContractLookupError(id, "Unknown contract"))
      } yield contract

  /** Supertype for all errors than may arise during the conversion. */
  sealed trait TransactionTreeConversionError extends Product with Serializable with PrettyPrinting

  object TransactionTreeConversionError {
    def toConversionError(error: TransactionError): TransactionTreeConversionError =
      error match {
        case TransactionError.DuplicateContractId(cid) =>
          DuplicateContractIdError(cid)
        case TransactionError.DuplicateContractKey(key) =>
          DuplicateContractKeyError(key)
        case TransactionError.AlreadyConsumed(cid, nodeId) =>
          AlreadyConsumedContractError(cid, nodeId.toString)
        case TransactionError.InconsistentContractKey(key) =>
          InconsistentContractKeyError(key)
        case TransactionError.EffectfulRollback(nodeIds) =>
          EffectfulRollbackError(nodeIds)
      }
  }

  final case class EffectfulRollbackError(nodeIds: Set[NodeId])
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[EffectfulRollbackError] =
      prettyOfClass(unnamedParam(_.nodeIds))
  }

  final case class DuplicateContractIdError(contractId: LfContractId)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[DuplicateContractIdError] =
      prettyOfClass(unnamedParam(_.contractId))
  }

  final case class DuplicateContractKeyError(key: LfGlobalKey)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[DuplicateContractKeyError] =
      prettyOfClass(unnamedParam(_.key))
  }

  final case class AlreadyConsumedContractError(contractId: LfContractId, nodeId: String)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[AlreadyConsumedContractError] = prettyOfClass(
      param("contractId", _.contractId),
      param("nodeId", _.nodeId.unquoted),
    )
  }

  final case class InconsistentContractKeyError(key: LfGlobalKey)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[InconsistentContractKeyError] =
      prettyOfClass(unnamedParam(_.key))
  }

  /** Indicates that a contract instance could not be looked up by an instance of
    * [[ContractInstanceOfId]].
    */
  final case class ContractLookupError(id: LfContractId, message: String)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[ContractLookupError] = prettyOfClass(
      param("id", _.id),
      param("message", _.message.unquoted),
    )
  }

  final case class SubmitterMetadataError(message: String) extends TransactionTreeConversionError {
    override protected def pretty: Pretty[SubmitterMetadataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  // TODO(i3013) Remove this error
  final case class ViewParticipantDataError(message: String)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[ViewParticipantDataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class MissingContractKeyLookupError(key: LfGlobalKey)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[MissingContractKeyLookupError] =
      prettyOfClass(unnamedParam(_.key))
  }

  final case class ContractKeyResolutionError(error: LegacyTransactionErrors.KeyInputError)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[ContractKeyResolutionError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class FailedToHashContact(error: String) extends TransactionTreeConversionError {
    override protected def pretty: Pretty[FailedToHashContact] = prettyOfString(_.error)
  }

  /** Indicates that too few salts have been supplied for creating a view */
  case object TooFewSalts extends TransactionTreeConversionError {
    override protected def pretty: Pretty[TooFewSalts.type] = prettyOfObject[TooFewSalts.type]
  }
  type TooFewSalts = TooFewSalts.type

  final case class UnknownPackageError(unknownTo: Seq[PackageUnknownTo])
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[UnknownPackageError] =
      prettyOfString(err => show"Some packages are not known to all informees.\n${err.unknownTo}")
  }

  final case class ConflictingPackagePreferenceError(
      conflicts: Map[LfPackageName, Set[LfPackageId]]
  ) extends TransactionTreeConversionError {
    override protected def pretty: Pretty[ConflictingPackagePreferenceError] = prettyOfString {
      err =>
        show"Detected conflicting package-ids for the same package name\n${err.conflicts}"
    }
  }

  final case class ContractIdAbsolutizationError(message: String)
      extends TransactionTreeConversionError {
    override protected def pretty: Pretty[ContractIdAbsolutizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class PackageUnknownTo(
      packageId: LfPackageId,
      participantId: ParticipantId,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[PackageUnknownTo] = prettyOfString { put =>
      show"Participant $participantId has not vetted ${put.packageId}"
    }
  }

}
