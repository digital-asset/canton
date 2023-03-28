// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.daml.error.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyMapping,
  TopologyStateElement,
  TopologyTransaction,
}

sealed trait TopologyManagerError extends CantonError

object TopologyManagerError extends TopologyManagerErrorGroup {

  @Explanation(
    """This error indicates that there was an internal error within the topology manager."""
  )
  @Resolution("Inspect error message for details.")
  object InternalError
      extends ErrorCode(
        id = "TOPOLOGY_MANAGER_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class ImplementMe()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "TODO(#11255) implement me"
        )
        with TopologyManagerError

    final case class Other(s: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"TODO(#11255) other failure: ${s}"
        )
        with TopologyManagerError

    final case class CryptoPublicError(error: CryptoPublicStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the public crypto store failed"
        )
        with TopologyManagerError

    final case class CryptoPrivateError(error: CryptoPrivateStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the secret crypto store failed"
        )
        with TopologyManagerError

    final case class IncompatibleOpMapping(op: TopologyChangeOp, mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The operation is incompatible with the mapping"
        )
        with TopologyManagerError

    final case class TopologySigningError(error: SigningError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Creating a signed transaction failed due to a crypto error"
        )
        with TopologyManagerError

    final case class ReplaceExistingFailed(invalid: ValidatedTopologyTransaction)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Replacing existing transaction failed upon removal"
        )
        with TopologyManagerError

  }

  @Explanation("""The topology manager has received a malformed message from another node.""")
  @Resolution("Inspect the error message for details.")
  object TopologyManagerAlarm extends AlarmErrorCode(id = "TOPOLOGY_MANAGER_ALARM") {
    final case class Warn(override val cause: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause)
        with TopologyManagerError {
      override lazy val logOnCreation: Boolean = false
    }
  }

  @Explanation(
    """This error indicates that the secret key with the respective fingerprint can not be found."""
  )
  @Resolution(
    "Ensure you only use fingerprints of secret keys stored in your secret key store."
  )
  object SecretKeyNotInStore
      extends ErrorCode(
        id = "SECRET_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Secret key with given fingerprint could not be found"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a command contained a fingerprint referring to a public key not being present in the public key store."""
  )
  @Resolution(
    "Upload the public key to the public key store using $node.keys.public.load(.) before retrying."
  )
  object PublicKeyNotInStore
      extends ErrorCode(
        id = "PUBLIC_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Public key with given fingerprint is missing in the public key store"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the uploaded signed transaction contained an invalid signature."""
  )
  @Resolution(
    "Ensure that the transaction is valid and uses a crypto version understood by this participant."
  )
  object InvalidSignatureError extends AlarmErrorCode(id = "INVALID_TOPOLOGY_TX_SIGNATURE_ERROR") {

    final case class Failure(error: SignatureCheckError)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = "Transaction signature verification failed")
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a transaction has already been added previously."""
  )
  @Resolution(
    """Nothing to do as the transaction is already registered. Note however that a revocation is " +
    final. If you want to re-enable a statement, you need to re-issue an new transaction."""
  )
  object DuplicateTransaction
      extends ErrorCode(
        id = "DUPLICATE_TOPOLOGY_TRANSACTION",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(
        transaction: TopologyTransaction[TopologyChangeOp],
        authKey: Fingerprint,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The given topology transaction already exists."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction would create a state that already exists and has been authorized with the same key."""
  )
  @Resolution("""Your intended change is already in effect.""")
  object MappingAlreadyExists
      extends ErrorCode(
        id = "TOPOLOGY_MAPPING_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(existing: TopologyStateElement[TopologyMapping], authKey: Fingerprint)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "A matching topology mapping authorized with the same key already exists in this state"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error results if the topology manager did not find a secret key in its store to authorize a certain topology transaction."""
  )
  @Resolution("""Inspect your topology transaction and your secret key store and check that you have the
      appropriate certificates and keys to issue the desired topology transaction. If the list of candidates is empty,
      then you are missing the certificates.""")
  object NoAppropriateSigningKeyInStore
      extends ErrorCode(
        id = "NO_APPROPRIATE_SIGNING_KEY_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(candidates: Seq[Fingerprint])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Could not find an appropriate signing key to issue the topology transaction"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the desired certificate could not be created."""
  )
  @Resolution("""Inspect the underlying error for details.""")
  object CertificateGenerationError
      extends ErrorCode(
        id = "CERTIFICATE_GENERATION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(error: X509CertificateError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to generate the certificate"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a transaction was rejected, as the signing key is not authorized within the current state."""
  )
  @Resolution(
    """Inspect the topology state and ensure that valid namespace or identifier delegations of the signing key exist or upload them before adding this transaction."""
  )
  object UnauthorizedTransaction extends AlarmErrorCode(id = "UNAUTHORIZED_TOPOLOGY_TRANSACTION") {

    final case class Failure()(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(cause = "Topology transaction is not properly authorized")
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a removal transaction was rejected, as the mapping / element affecting the removal did not exist."""
  )
  @Resolution(
    """Inspect the topology state and ensure the mapping and the element id of the active transaction you are trying to revoke matches your revocation arguments."""
  )
  object NoCorrespondingActiveTxToRevoke
      extends ErrorCode(
        id = "NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Mapping(mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the mapping of the revocation request"
        )
        with TopologyManagerError
    final case class Element(element: TopologyStateElement[TopologyMapping])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the element of the revocation request"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would remove the last valid key of the given entity, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingLastKeyMustBeForced
      extends ErrorCode(
        id = "REMOVING_LAST_KEY_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Topology transaction would remove the last key of the given entity"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would create dangling topology transactions, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingKeyWithDanglingTransactionsMustBeForced
      extends ErrorCode(
        id = "REMOVING_KEY_DANGLING_TRANSACTIONS_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Topology transaction would remove a key that creates conflicts and dangling transactions"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that it has been attempted to increase the ``ledgerTimeRecordTimeTolerance`` domain parameter in an insecure manner.
      |Increasing this parameter may disable security checks and can therefore be a security risk.
      |"""
  )
  @Resolution(
    """Make sure that the new value of ``ledgerTimeRecordTimeTolerance`` is at most half of the ``mediatorDeduplicationTimeout`` domain parameter.
      |
      |Use ``myDomain.service.set_ledger_time_record_time_tolerance`` for securely increasing ledgerTimeRecordTimeTolerance.
      |
      |Alternatively, add the ``force = true`` flag to your command, if security is not a concern for you.
      |The security checks will be effective again after twice the new value of ``ledgerTimeRecordTimeTolerance``.
      |Using ``force = true`` is safe upon domain bootstrapping.
      |"""
  )
  object IncreaseOfLedgerTimeRecordTimeTolerance
      extends ErrorCode(
        id = "INCREASE_OF_LEDGER_TIME_RECORD_TIME_TOLERANCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class TemporarilyInsecure(
        oldValue: NonNegativeFiniteDuration,
        newValue: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The parameter ledgerTimeRecordTimeTolerance can currently not be increased to $newValue."
        )
        with TopologyManagerError

    final case class PermanentlyInsecure(
        newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
        mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Unable to increase ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance, because it must not be more than half of mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
        )
        with TopologyManagerError
  }

  abstract class DomainErrorGroup extends ErrorGroup()
  abstract class ParticipantErrorGroup extends ErrorGroup()

}
