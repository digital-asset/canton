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
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractInstanceOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.store.ContractLookup
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.{GenContractInstance, *}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractHasher
import com.digitalasset.daml.lf.transaction.LegacyTransactionErrors

import java.util.UUID
import scala.concurrent.ExecutionContext

trait TransactionTreeFactory {

  /** The [[com.digitalasset.canton.protocol.CantonContractIdVersion]] to be used for newly created
    * contracts
    */
  def cantonContractIdVersion: CantonContractIdVersion

  /** Converts a `transaction: LfTransaction` to the corresponding transaction tree, if possible.
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
      maxSequencingTime: CantonTimestamp,
      validatePackageVettings: Boolean,
      protocolLimits: TransactionProtocolLimits,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree]

  /** Reconstructs a transaction view from a reinterpreted action description, using the supplied
    * salts.
    * @throws java.lang.IllegalArgumentException
    *   if `transaction` does not contain exactly one root node
    */
  def tryReconstruct(
      transaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
      submittingParticipantO: Option[ParticipantId],
      salts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: ContractInstanceOfId,
      rbContext: RollbackContext,
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
    new NextGenTransactionTreeFactory(
      submittingParticipant,
      synchronizerId,
      cantonContractIdVersion,
      cryptoOps,
      hasher,
      loggerFactory,
    )

  def contractInstanceLookup(
      contractStore: ContractLookup
  )(implicit ex: ExecutionContext, traceContext: TraceContext): ContractInstanceOfId = { id =>
    contractStore
      .lookup(id)
      .collect { case c: GenContractInstance => c: GenContractInstance }
      .toRight(ContractLookupError(id, "Unknown contract"))
  }

  /** Supertype for all errors than may arise during the conversion. */
  sealed trait TransactionTreeConversionError
      extends Product
      with Serializable
      with PrettyPrintingFromCompanion

  final case class TransactionViewLimitError(message: String)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[TransactionViewLimitError] =
      TransactionViewLimitError
  }

  object TransactionViewLimitError extends PrettyPrintingCompanion[TransactionViewLimitError] {
    override protected val pretty: Pretty[TransactionViewLimitError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that a contract instance could not be looked up by an instance of
    * [[ContractInstanceOfId]].
    */
  final case class ContractLookupError(id: LfContractId, message: String)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[ContractLookupError] = ContractLookupError
  }

  object ContractLookupError extends PrettyPrintingCompanion[ContractLookupError] {
    override protected val pretty: Pretty[ContractLookupError] = prettyOfClass(
      param("id", _.id),
      param("message", _.message.unquoted),
    )
  }

  final case class SubmitterMetadataError(message: String) extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[SubmitterMetadataError] =
      SubmitterMetadataError
  }

  object SubmitterMetadataError extends PrettyPrintingCompanion[SubmitterMetadataError] {
    override protected val pretty: Pretty[SubmitterMetadataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class RolledBackEffect(viewPosition: ViewPosition)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[RolledBackEffect] = RolledBackEffect
  }

  object RolledBackEffect extends PrettyPrintingCompanion[RolledBackEffect] {
    override protected val pretty: Pretty[RolledBackEffect] = prettyOfClass(
      param("view position", _.viewPosition)
    )
  }

  // TODO(i3013) Remove this error
  final case class ViewParticipantDataError(message: String)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[ViewParticipantDataError] =
      ViewParticipantDataError
  }

  object ViewParticipantDataError extends PrettyPrintingCompanion[ViewParticipantDataError] {
    override protected val pretty: Pretty[ViewParticipantDataError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class MissingContractKeyLookupError(key: LfGlobalKey)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[MissingContractKeyLookupError] =
      MissingContractKeyLookupError
  }

  object MissingContractKeyLookupError
      extends PrettyPrintingCompanion[MissingContractKeyLookupError] {
    override protected val pretty: Pretty[MissingContractKeyLookupError] =
      prettyOfClass(unnamedParam(_.key))
  }

  final case class ContractKeyResolutionError(error: LegacyTransactionErrors.KeyInputError)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[ContractKeyResolutionError] =
      ContractKeyResolutionError
  }

  object ContractKeyResolutionError extends PrettyPrintingCompanion[ContractKeyResolutionError] {
    override protected val pretty: Pretty[ContractKeyResolutionError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class FailedToHashContact(error: String) extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[FailedToHashContact] = FailedToHashContact
  }

  object FailedToHashContact extends PrettyPrintingCompanion[FailedToHashContact] {
    override protected val pretty: Pretty[FailedToHashContact] = prettyOfString(_.error)
  }

  /** Indicates that too few salts have been supplied for creating a view */
  case object TooFewSalts extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[TooFewSalts.this.type] =
      TooFewSaltsPrettyPrintingCompanion
  }

  private object TooFewSaltsPrettyPrintingCompanion
      extends PrettyPrintingCompanion[TooFewSalts.type] {
    override protected val pretty: Pretty[TooFewSalts.type] = prettyOfObject[TooFewSalts.type]
  }
  type TooFewSalts = TooFewSalts.type

  final case class UnknownPackageError(unknownTo: Seq[PackageUnknownTo])
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[UnknownPackageError] = UnknownPackageError
  }

  object UnknownPackageError extends PrettyPrintingCompanion[UnknownPackageError] {
    override protected val pretty: Pretty[UnknownPackageError] =
      prettyOfString(err => show"Some packages are not known to all informees.\n${err.unknownTo}")
  }

  final case class ConflictingPackagePreferenceError(
      conflicts: Map[LfPackageName, Set[LfPackageId]]
  ) extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[ConflictingPackagePreferenceError] =
      ConflictingPackagePreferenceError
  }

  object ConflictingPackagePreferenceError
      extends PrettyPrintingCompanion[ConflictingPackagePreferenceError] {
    override protected val pretty: Pretty[ConflictingPackagePreferenceError] = prettyOfString {
      err =>
        show"Detected conflicting package-ids for the same package name\n${err.conflicts}"
    }
  }

  /** Indicates that a constructed view failed validation, e.g. because the submitted transaction
    * records conflicting outputs for the same external call.
    */
  final case class InvalidTransactionViewError(message: String)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[InvalidTransactionViewError] =
      InvalidTransactionViewError
  }

  object InvalidTransactionViewError extends PrettyPrintingCompanion[InvalidTransactionViewError] {
    override protected val pretty: Pretty[InvalidTransactionViewError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class ContractIdAbsolutizationError(message: String)
      extends TransactionTreeConversionError {
    override def prettyCompanion: PrettyPrintingCompanion[ContractIdAbsolutizationError] =
      ContractIdAbsolutizationError
  }

  object ContractIdAbsolutizationError
      extends PrettyPrintingCompanion[ContractIdAbsolutizationError] {
    override protected val pretty: Pretty[ContractIdAbsolutizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class PackageUnknownTo(
      packageId: LfPackageId,
      participantId: ParticipantId,
  ) extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[PackageUnknownTo] = PackageUnknownTo
  }

  object PackageUnknownTo extends PrettyPrintingCompanion[PackageUnknownTo] {
    override protected val pretty: Pretty[PackageUnknownTo] = prettyOfString { put =>
      show"Participant ${put.participantId} has not vetted ${put.packageId}"
    }
  }

}
