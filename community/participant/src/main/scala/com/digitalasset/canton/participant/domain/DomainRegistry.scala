// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import com.daml.error.{ErrorCategory, ErrorCode, ErrorGroup, Explanation, Resolution}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateFactory,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.DomainRegistryErrorGroup
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

/** A registry of domains. */
trait DomainRegistry extends AutoCloseable {

  /**  Returns a domain handle that is used to setup a connection to a new domain
    */
  def connect(
      config: DomainConnectionConfig,
      syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, DomainHandle]]

  /** Returns a builder for the sequencer client */
  def sequencerConnectClientBuilder: SequencerConnectClient.Builder

}

sealed trait DomainRegistryError extends Product with Serializable with CantonError

object DomainRegistryError extends DomainRegistryErrorGroup {

  object ConnectionErrors extends ErrorGroup() {

    @Explanation(
      "This error results if the GRPC connection to the domain service fails with GRPC status UNAVAILABLE."
    )
    @Resolution(
      "Check your connection settings and ensure that the domain can really be reached."
    )
    object DomainIsNotAvailable
        extends ErrorCode(id = "DOMAIN_IS_NOT_AVAILABLE", ErrorCategory.TransientServerFailure) {
      case class Error(alias: DomainAlias, reason: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"Cannot connect to domain ${alias}")
          with DomainRegistryError
    }

    @Explanation(
      """This error indicates that the connecting participant has either not yet been activated by the domain operator.
        If the participant was previously successfully connected to the domain, then this error indicates that the domain
        operator has deactivated the participant."""
    )
    @Resolution(
      "Contact the domain operator and inquire the permissions your participant node has on the given domain."
    )
    object ParticipantIsNotActive
        extends ErrorCode(
          id = "PARTICIPANT_IS_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(serverResponse: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The participant is not yet active")
          with DomainRegistryError
    }

    @Explanation(
      """This error indicates that the participant failed to connect to the sequencer."""
    )
    @Resolution("Inspect the provided reason.")
    object FailedToConnectToSequencer
        extends ErrorCode(
          id = "FAILED_TO_CONNECT_TO_SEQUENCER",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The participant failed to connect to the sequencer")
          with DomainRegistryError
    }
    @Explanation(
      """This error indicates that the participant failed to connect due to a general GRPC error."""
    )
    @Resolution("Inspect the provided reason and contact support.")
    object GrpcFailure
        extends ErrorCode(
          id = "GRPC_CONNECTION_FAILURE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(error: GrpcError)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The domain connection attempt failed with a GRPC error")
          with DomainRegistryError
    }

  }

  object ConfigurationErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that the domain this participant is trying to connect to is a domain where unique
        contract keys are supported, while this participant is already connected to other domains. Multiple domains and 
        unique contract keys are mutually exclusive features."""
    )
    @Resolution("Use isolated participants for domains that require unique keys.")
    object IncompatibleUniqueContractKeysMode
        extends ErrorCode(
          id = "INCOMPATIBLE_UNIQUE_CONTRACT_KEYS_MODE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(override val cause: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause)
          with DomainRegistryError {}
    }

    @Explanation(
      """This error indicates that the participant can not issue a domain trust certificate. Such a certificate is 
        |necessary to become active on a domain. Therefore, it must be present in the authorized store of the 
        |participant topology manager."""
    )
    @Resolution(
      """Manually upload a valid domain trust certificate for the given domain or upload
        |the necessary certificates such that participant can issue such certificates automatically."""
    )
    object CanNotIssueDomainTrustCertificate
        extends ErrorCode(
          id = "CANNOT_ISSUE_DOMAIN_TRUST_CERTIFICATE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error()(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(
            cause = "Can not auto-issue a domain-trust certificate on this node."
          )
          with DomainRegistryError {}
    }
    @Explanation(
      """This error indicates there is a validation error with the configured connections for the domain"""
    )
    object InvalidDomainConnections
        extends ErrorCode(
          id = "INVALID_DOMAIN_CONNECTION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(message: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Configured domain connection is invalid")
          with DomainRegistryError
    }

    @Explanation(
      "Error indicating that the domain parameters have been changed, while this isn't supported yet."
    )
    object DomainParametersChanged
        extends ErrorCode(
          id = "DOMAIN_PARAMETERS_CHANGED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(
          oldParameters: Option[StaticDomainParameters],
          newParameters: StaticDomainParameters,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"The domain parameters have changed")
          with DomainRegistryError

      override def logLevel: Level = Level.WARN

    }
  }

  object HandshakeErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that the domain is using crypto settings which are 
                                either not supported or not enabled on this participant."""
    )
    @Resolution(
      "Consult the error message and adjust the supported crypto schemes of this participant."
    )
    object DomainCryptoHandshakeFailed
        extends ErrorCode(
          id = "DOMAIN_CRYPTO_HANDSHAKE_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Crypto method handshake with domain failed")
          with DomainRegistryError
    }

    @Explanation(
      """This error indicates that the domain requires the participant to accept a
                                service agreement before connecting to it."""
    )
    @Resolution(
      "Use the commands $participant.domains.get_agreement and $participant.domains.accept_agreement to accept the agreement."
    )
    object ServiceAgreementAcceptanceFailed
        extends ErrorCode(
          id = "SERVICE_AGREEMENT_ACCEPTANCE_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Service agreement failed")
          with DomainRegistryError
    }

    // TODO(i5990) actually figure out what the failure reasons are and distinguish them between internal and normal
    @Explanation(
      """This error indicates that the participant to domain handshake has failed."""
    )
    @Resolution("Inspect the provided reason for more details and contact support.")
    object HandshakeFailed
        extends ErrorCode(
          id = "DOMAIN_HANDSHAKE_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Handshake with domain has failed")
          with DomainRegistryError
    }

    @Explanation(
      """This error indicates that the domain-id does not match the one that the 
        participant expects. If this error happens on a first connect, then the domain id 
        defined in the domain connection settings does not match the remote domain.
        If this happens on a reconnect, then the remote domain has been reset for some reason."""
    )
    @Resolution("Carefully verify the connection settings.")
    object DomainIdMismatch
        extends ErrorCode(
          id = "DOMAIN_ID_MISMATCH",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(expected: DomainId, observed: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "The domain reports a different domain-id than the participant is expecting"
          )
          with DomainRegistryError
    }

    @Explanation("""This error indicates that the domain alias was previously used to
        connect to a domain with a different domain id. This is a known situation when an existing participant
        is trying to connect to a freshly re-initialised domain.""")
    @Resolution("Carefully verify the connection settings.")
    object DomainAliasDuplication
        extends ErrorCode(
          id = "DOMAIN_ALIAS_DUPLICATION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Error(domainId: DomainId, alias: DomainAlias, expectedDomainId: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              "The domain with the given alias reports a different domain id than the participant is expecting"
          )
          with DomainRegistryError
    }
  }

  @Explanation(
    "This error indicates that there was an error converting topology transactions during connecting to a domain."
  )
  @Resolution("Contact the operator of the topology management for this node.")
  object TopologyConversionError
      extends ErrorCode(
        id = "TOPOLOGY_CONVERSION_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {

    case class Error(override val cause: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause)
        with DomainRegistryError
  }

  @Explanation(
    """This error indicates that there has been an internal error noticed by Canton."""
  )
  @Resolution("Contact support and provide the failure reason.")
  object DomainRegistryInternalError
      extends ErrorCode(
        id = "DOMAIN_REGISTRY_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class InitialOnboardingError(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError

    case class TopologyHandshakeError(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Handshake with remote topology transaction registrations service failed",
          throwableO = Some(throwable),
        )
        with DomainRegistryError
    case class InvalidResponse(
        override val cause: String,
        override val throwableO: Option[Throwable],
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause, throwableO)
        with DomainRegistryError
    case class DeserializationFailure(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError
    case class InvalidState(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError
    case class FailedToAddParticipantDomainStateCert(reason: ParticipantTopologyManagerError)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to issue domain state cert due to an unexpected reason"
        )
        with DomainRegistryError
  }

}

/** A context handle serving all necessary information / connectivity utilities for the node to setup a connection to a
  * new domain
  */
trait DomainHandle extends AutoCloseable {

  /** Client to the domain's sequencer. */
  def sequencerClient: SequencerClient

  def staticParameters: StaticDomainParameters

  def domainId: DomainId

  def domainAlias: DomainAlias

  def topologyClient: DomainTopologyClientWithInit

  def topologyStore: TopologyStore[TopologyStoreId.DomainStore]

  def domainPersistentState: SyncDomainPersistentState
}
