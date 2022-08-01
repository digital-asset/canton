// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.error.{ErrorCategory, ErrorGroup, ErrorResource, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.v0.LocalReject.Code
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.empty
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

/** Possible verdicts on a transaction view from the participant's perspective.
  * The verdict can be `LocalApprove`, `LocalReject` or `Malformed`.
  * The verdicts `LocalReject` and `Malformed` include a `reason` pointing out which checks in Phase 3 have failed.
  */
sealed trait LocalVerdict extends Product with Serializable with PrettyPrinting {
  def toProtoV0: v0.LocalVerdict

}

case object LocalApprove extends LocalVerdict {
  def toProtoV0: v0.LocalVerdict =
    v0.LocalVerdict(v0.LocalVerdict.SomeLocalVerdict.LocalApprove(empty.Empty()))

  override def pretty: Pretty[this.type] = prettyOfObject[LocalApprove.type]

}

trait HasResourceSeq {

  def _resource: Seq[String] = Seq()

}

trait LocalReject
    extends LocalVerdict
    with HasResourceSeq
    with TransactionErrorWithEnum[v0.LocalReject.Code] {

  def toProtoV0: v0.LocalVerdict =
    v0.LocalVerdict(v0.LocalVerdict.SomeLocalVerdict.LocalReject(toLocalRejectProtoV0))

  def toLocalRejectProtoV0: v0.LocalReject = v0.LocalReject(code.protoCode, reason, _resource)

  def reason: String = ""

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("id", _.code.id.unquoted),
      param("reason", _.reason.unquoted),
      param("resource", _._resource.map(_.unquoted)),
    )

}

object LocalReject extends LocalRejectionGroup {

  // list of local errors, used to map them during transport
  // if you add a new error below, you must add it to this list here as well

  def fromProtoV0(v: v0.LocalReject): ParsingResult[LocalReject] = {
    import ConsistencyRejections._
    v.code match {
      case Code.MissingCode => Left(FieldNotSet("LocalReject.code"))
      case Code.LockedContracts => Right(LockedContracts.Reject(v.resource))
      case Code.LockedKeys => Right(LockedKeys.Reject(v.resource))
      case Code.InactiveContracts => Right(InactiveContracts.Reject(v.resource))
      case Code.DuplicateKey => Right(DuplicateKey.Reject(v.resource))
      case Code.CreatesExistingContract => Right(CreatesExistingContracts.Reject(v.resource))
      case Code.LedgerTime => Right(TimeRejects.LedgerTime.Reject(v.reason))
      case Code.SubmissionTime => Right(TimeRejects.SubmissionTime.Reject(v.reason))
      case Code.LocalTimeout => Right(TimeRejects.LocalTimeout.Reject())
      case Code.MalformedPayloads => Right(MalformedRejects.Payloads.Reject(v.reason))
      case Code.MalformedModel => Right(MalformedRejects.ModelConformance.Reject(v.reason))
      case Code.MalformedConfirmationPolicy =>
        Right(MalformedRejects.MultipleConfirmationPolicies.Reject(v.reason))
      case Code.BadRootHashMessage => Right(MalformedRejects.BadRootHashMessages.Reject(v.reason))
      case Code.TransferOutActivenessCheck =>
        Right(TransferOutRejects.ActivenessCheckFailed.Reject(v.reason))
      case Code.TransferInAlreadyCompleted =>
        Right(TransferInRejects.AlreadyCompleted.Reject(v.reason))
      case Code.TransferInAlreadyActive =>
        Right(TransferInRejects.ContractAlreadyActive.Reject(v.reason))
      case Code.TransferInAlreadyArchived =>
        Right(TransferInRejects.ContractAlreadyArchived.Reject(v.reason))
      case Code.TransferInLocked => Right(TransferInRejects.ContractIsLocked.Reject(v.reason))
      case Code.InconsistentKey => Right(InconsistentKey.Reject(v.resource))
      case Code.Unrecognized(code) =>
        Left(
          ValueDeserializationError(
            "reject",
            s"Unknown local rejection error code ${code} with ${v.reason}",
          )
        )
    }
  }

  type LocalVerdictErrorCode = ErrorCodeWithEnum[v0.LocalReject.Code]

  abstract class LocalVerdictError(
      cause: String,
      throwableO: Option[Throwable] = None,
      resourceType: Option[ErrorResource] = None,
  )(implicit code: ErrorCodeWithEnum[v0.LocalReject.Code])
      extends TransactionErrorWithEnumImpl[v0.LocalReject.Code](
        cause,
        throwableO,
        // Local verdicts are always reported asynchronously and therefore covered by the submission rank check.
        definiteAnswer = true,
      )(code)
      with HasResourceSeq {

    override def resources: Seq[(ErrorResource, String)] =
      resourceType.fold(Seq.empty[(ErrorResource, String)])(rt => _resource.map(rs => (rt, rs)))
  }

  object ConsistencyRejections extends ErrorGroup() {
    @Explanation(
      """The transaction is referring to locked contracts which are in the process of being
        created, transferred, or archived by another transaction. If the other transaction fails, this transaction could be successfully retried."""
    )
    @Resolution("Retry the transaction")
    object LockedContracts
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_LOCKED_CONTRACTS",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LockedContracts,
        ) {

      case class Reject(override val _resource: Seq[String])
          extends LocalVerdictError(
            cause = s"Rejected transaction is referring to locked contracts ${_resource}",
            resourceType = Some(CantonErrorResource.ContractId),
          )
          with LocalReject
    }

    @Explanation(
      """The transaction is referring to locked keys which are in the process of being
        modified by another transaction."""
    )
    @Resolution("Retry the transaction")
    object LockedKeys
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_LOCKED_KEYS",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LockedKeys,
        ) {
      case class Reject(override val _resource: Seq[String])
          extends LocalVerdictError(
            cause = "Rejected transaction is referring to locked keys",
            resourceType = Some(CantonErrorResource.ContractKey),
          )
          with LocalReject
    }

    @Explanation(
      """The transaction is referring to contracts that have either been previously 
                                archived, transferred to another domain, or do not exist."""
    )
    @Resolution("Inspect your contract state and try a different transaction.")
    object InactiveContracts
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_INACTIVE_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.InactiveContracts,
        ) {
      case class Reject(override val _resource: Seq[String])
          extends LocalVerdictError(
            cause = "Rejected transaction is referring to inactive contracts",
            resourceType = Some(CantonErrorResource.ContractId),
          )
          with LocalReject
    }

    @Explanation(
      """If the participant provides unique contract key support, 
         this error will indicate that a transaction would create a unique key which already exists."""
    )
    @Resolution(
      "It depends on your use case and application whether and when retrying makes sense or not."
    )
    object DuplicateKey
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_DUPLICATE_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.DuplicateKey,
        ) {
      case class Reject(override val _resource: Seq[String])
      // Error message contains the term: "Inconsistent" and "DuplicateKey" to avoid failing contract key ledger api conformance tests
          extends LocalVerdictError(
            cause =
              "Inconsistent rejected transaction would create a key that already exists (DuplicateKey)",
            resourceType = Some(CantonErrorResource.ContractKey),
          )
          with LocalReject
    }

    @Explanation(
      """If the participant provides unique contract key support, 
         this error will indicate that a transaction expected a key to be unallocated, but a contract for the key already exists."""
    )
    @Resolution(
      "It depends on your use case and application whether and when retrying makes sense or not."
    )
    object InconsistentKey
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_INCONSISTENT_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.InconsistentKey,
        ) {
      case class Reject(override val _resource: Seq[String])
          extends LocalVerdictError(
            cause =
              "Inconsistent rejected transaction expected unassigned key, which already exists",
            resourceType = Some(CantonErrorResource.ContractKey),
          )
          with LocalReject
    }

    @Explanation(
      """This error indicates that the transaction would create already existing contracts."""
    )
    @Resolution("This error indicates either faulty or malicious behaviour.")
    object CreatesExistingContracts
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_CREATES_EXISTING_CONTRACTS",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.CreatesExistingContract,
        ) {
      case class Reject(override val _resource: Seq[String])
          extends LocalVerdictError(
            cause = "Rejected transaction would create contract(s) that already exist",
            resourceType = Some(CantonErrorResource.ContractKey),
          )
          with LocalReject
    }

  }

  object TimeRejects extends ErrorGroup() {

    @Explanation(
      """This error is thrown if the ledger time and the record time differ more than permitted. 
        This can happen in an overloaded system due to high latencies or for transactions with long interpretation times."""
    )
    @Resolution(
      "For long-running transactions, specify a ledger time with the command submission. For short-running transactions, simply retry."
    )
    object LedgerTime
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_LEDGER_TIME_OUT_OF_BOUND",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LedgerTime,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause =
              "Rejected transaction as delta of the ledger time and the record time exceed the time tolerance"
          )
          with LocalReject
    }

    @Explanation(
      """This error is thrown if the submission time and the record time differ more than permitted.
        This can happen in an overloaded system due to high latencies or for transactions with long interpretation times."""
    )
    @Resolution(
      "For long-running transactions, adjust the ledger time bounds used with the command submission. For short-running transactions, simply retry."
    )
    object SubmissionTime
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_SUBMISSION_TIME_OUT_OF_BOUND",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.SubmissionTime,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause =
              "Rejected transaction as delta of the submission time and the record time exceed the time tolerance"
          )
          with LocalReject
    }

    @Explanation(
      """This rejection is sent if the participant locally determined a timeout."""
    )
    @Resolution("""In the first instance, resubmit your transaction.
        | If the rejection still appears spuriously, consider increasing the `participantResponseTimeout` or 
        | `mediatorReactionTimeout` values in the `DynamicDomainParameters`.
        | If the rejection appears unrelated to timeout settings, validate that all other Canton components
        |   which take part in the transaction also function correctly and that, e.g., messages are not stuck at the
        |   sequencer.
        |""")
    object LocalTimeout
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_TIMEOUT",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LocalTimeout,
        ) {
      override def logLevel: Level = Level.WARN
      case class Reject()
          extends LocalVerdictError(
            cause = "Rejected transaction due to a participant determined timeout"
          )
          with LocalReject
    }

  }

  sealed trait Malformed extends LocalReject

  object MalformedRejects extends ErrorGroup() {

    @Explanation(
      """This rejection is made by a participant if a view of the transaction is malformed."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object Payloads
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_MALFORMED_PAYLOAD",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.MalformedPayloads,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transaction due to malformed payload within views"
          )
          with Malformed
    }

    @Explanation(
      """This rejection is made by a participant if a transaction fails a model conformance check."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object ModelConformance
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.MalformedModel,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transaction due to model conformance check failed"
          )
          with Malformed
    }

    @Explanation(
      """This rejection is made by a participant if a transaction uses different confirmation policies per view."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object MultipleConfirmationPolicies
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_DETECTED_MULTIPLE_CONFIRMATION_POLICIES",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.MalformedConfirmationPolicy,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transaction due to multiple confirmation policies"
          )
          with Malformed
    }

    @Explanation(
      """This rejection is made by a participant if a transaction does not contain valid root hash messages."""
    )
    @Resolution(
      "This indicates a race condition due to a in-flight topology change, or malicious or faulty behaviour."
    )
    object BadRootHashMessages
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_BAD_ROOT_HASH_MESSAGES",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.BadRootHashMessage,
        ) {
      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transaction due to bad root hash error messages."
          )
          with Malformed
    }

    @Explanation(
      """This rejection is emitted by a participant if it receives an aggregated reject without any reason."""
    )
    @Resolution("This indicates either malicious or faulty mediator.")
    object EmptyRejection
        extends LocalVerdictErrorCode(
          id = "LOCAL_VERDICT_EMPTY_REJECTION",
          ErrorCategory.MaliciousOrFaultyBehaviour,
          v0.LocalReject.Code.MissingCode,
        ) {
      case class Reject()
          extends LocalVerdictError(cause = "Rejected transaction due to empty aggregated reject.")
          with Malformed {

        override def toLocalRejectProtoV0: v0.LocalReject = {
          LoggerFactory
            .getLogger(getClass)
            .error(
              "empty rejections are for internal use only and should never be sent over the wire"
            )
          super.toLocalRejectProtoV0
        }

      }
    }

  }

  object TransferOutRejects extends ErrorGroup() {

    @Explanation(
      """Activeness check failed for transfer out submission. This rejection occurs if the contract to be  
        |transferred has already been transferred or is currently locked (due to a competing transaction)
        |on  domain."""
    )
    @Resolution(
      "Depending on your use-case and your expectation, retry the transaction."
    )
    object ActivenessCheckFailed
        extends LocalVerdictErrorCode(
          id = "TRANSFER_OUT_ACTIVENESS_CHECK_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.TransferOutActivenessCheck,
        ) {

      case class Reject(override val reason: String)
          extends LocalVerdictError(cause = "Activeness check failed.")
          with LocalReject

    }

  }

  object TransferInRejects extends ErrorGroup() {
    @Explanation(
      """This rejection is emitted by a participant if a transfer would be invoked on an already archived contract."""
    )
    object ContractAlreadyArchived
        extends LocalVerdictErrorCode(
          id = "TRANSFER_IN_CONTRACT_ALREADY_ARCHIVED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.TransferInAlreadyArchived,
        ) {

      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transfer as transferred contract is already archived."
          )
          with LocalReject

    }
    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been made by another entity."""
    )
    object ContractAlreadyActive
        extends LocalVerdictErrorCode(
          id = "TRANSFER_IN_CONTRACT_ALREADY_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.TransferInAlreadyActive,
        ) {

      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transfer as the contract is already active on the target domain."
          )
          with LocalReject

    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in is referring to an already locked contract."""
    )
    object ContractIsLocked
        extends LocalVerdictErrorCode(
          id = "TRANSFER_IN_CONTRACT_IS_LOCKED",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.TransferInLocked,
        ) {

      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transfer as the transferred contract is locked."
          )
          with LocalReject

    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been completed."""
    )
    object AlreadyCompleted
        extends LocalVerdictErrorCode(
          id = "TRANSFER_IN_ALREADY_COMPLETED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.TransferInAlreadyCompleted,
        ) {

      case class Reject(override val reason: String)
          extends LocalVerdictError(
            cause = "Rejected transfer as the transfer has already completed"
          )
          with LocalReject

    }

  }

}

object LocalVerdict {
  def fromProtoV0(
      localVerdictP: v0.LocalVerdict
  ): ParsingResult[LocalVerdict] = {
    import v0.LocalVerdict.{SomeLocalVerdict => Lv}
    localVerdictP match {
      case v0.LocalVerdict(Lv.LocalApprove(empty.Empty(_))) => Right(LocalApprove)
      case v0.LocalVerdict(Lv.LocalReject(value)) => LocalReject.fromProtoV0(value)
      case v0.LocalVerdict(Lv.Empty) =>
        Left(OtherError("Unable to deserialize LocalVerdict, as the content is empty"))
    }
  }
}
