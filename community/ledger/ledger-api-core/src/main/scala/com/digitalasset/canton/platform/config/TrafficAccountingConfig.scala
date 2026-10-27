// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.config.TopologyConfigConverters.*
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.nonempty.NonEmpty
import com.digitalasset.nonempty.NonEmptyUtil.instances.*
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

/** Base configuration for the user traffic enforcement in the participant node. Currently, only the
  * internal, in-process server variant is supported, allowing for later an external TEA server to
  * be configured as well.
  *
  * @param enabled
  *   Whether to enable the traffic enforcement feature. Note: This feature is experimental,
  *   unstable and is disabled by default. Enabling it in production environments is not
  *   recommended. Disabled by default
  * @param enforceCostOnSubmissions
  *   Whether to enforce traffic cost on submissions. If enabled, the participant will validate that
  *   the account associated with the submission is correctly permissioned and has sufficient
  *   balance to cover the expected traffic cost. If the account has insufficient balance, the
  *   submission will be rejected. When disabled, the participant still stores which account to
  *   debit before sending the submission, just without the balance check. Disabled by default.
  * @param rejectMultiPartySubmissions
  *   Whether to reject submissions that have several actAs parties and no account. TEA accounts are
  *   bound to a single party, so by default the participant stores a no-account marker for such a
  *   submission, and it isn't debited (an informational message is logged). TEA records its
  *   completion as uncharged. When enabled, such submissions are rejected instead. Disabled by
  *   default.
  * @param allowSubmissionsOnDegradation
  *   Whether to allow a submission to proceed without a balance check when the account lookup or
  *   the write of the submission's account fails, for example due to a database outage. The write
  *   may still commit even if the response is lost, in which case TEA charges the completion as
  *   usual. If it doesn't commit, TEA has no stored account and records any positive-cost
  *   completion as uncharged, with a missing-correlation warning. Doesn't apply when the traffic
  *   service refuses the request or reports a conflicting earlier submission. No-account marker
  *   writes are best effort regardless of what this is set to. A bypassed check is logged at WARN.
  *   Disabled by default, so a failed lookup or write fails the submission.
  * @param debitRejectedCompletions
  *   Whether to charge rejected completions. They can still have a positive cost if the sequencer
  *   consumed traffic, for example when a charged request returns as a `DeliverError`. When
  *   disabled, TEA records these completions as uncharged. Enabled by default.
  * @param trafficEnforcementServer
  *   The configuration for the connection to the traffic server. Currently, only the internal,
  *   in-process server variant is supported.
  */
final case class TrafficAccountingConfig(
    enabled: Boolean = false,
    enforceCostOnSubmissions: Boolean = false,
    rejectMultiPartySubmissions: Boolean = false,
    allowSubmissionsOnDegradation: Boolean = false,
    debitRejectedCompletions: Boolean = true,
    trafficEnforcementServer: TrafficEnforcementServerConfig =
      TrafficEnforcementServerConfig.Internal(),
) {

  /** Returns true if traffic accounting and DamlCredit top-up is enabled.
    */
  def damlTopUpEnabled: Boolean =
    enabled && (trafficEnforcementServer match {
      case internal: TrafficEnforcementServerConfig.Internal =>
        internal.topUp match {
          case TopUpConfig.Disabled => false
          case _: TopUpConfig.DamlCredit => true
        }
    })
}

sealed trait TrafficEnforcementServerConfig

object TrafficEnforcementServerConfig {

  /** Configuration for the internal, in-process traffic enforcement server.
    *
    * @param inProcessTeaServerName
    *   The name of the in-process gRPC serving the traffic service API of the traffic enforcement
    *   server
    * @param databaseQueryTimeout
    *   Per-attempt deadline for the single database read backing `GetAccount`. Enforced on
    *   PostgreSQL via `SET LOCAL statement_timeout`, which is expressed in whole milliseconds, so
    *   this must be at least one millisecond. Has no effect on H2 (no millisecond-level query
    *   timeout setting).
    * @param accountLookupTimeout
    *   Total per-call budget for the `GetAccount` RPC, covering the client's retries around
    *   `databaseQueryTimeout`. Must be strictly greater than `databaseQueryTimeout`, so a timed-out
    *   query still leaves room for a retry. Also bounds the `RegisterSubmission` call, which isn't
    *   retried.
    * @param topUp
    *   How traffic top-ups are ingested. Disabled by default.
    */
  final case class Internal(
      inProcessTeaServerName: String = "TeaGrpcInProcServer",
      projection: ProjectionConfig = ProjectionConfig(),
      databaseQueryTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
      accountLookupTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(20),
      topUp: TopUpConfig = TopUpConfig.Disabled,
  ) extends TrafficEnforcementServerConfig {
    require(
      databaseQueryTimeout.underlying.toMillis > 0,
      s"databaseQueryTimeout ($databaseQueryTimeout) must be at least one millisecond",
    )
    require(
      databaseQueryTimeout.duration < accountLookupTimeout.duration,
      s"databaseQueryTimeout ($databaseQueryTimeout) must be strictly less than accountLookupTimeout" +
        s" ($accountLookupTimeout)",
    )

    def processServerNameForInstance(instance: InstanceName, ledgerApiPort: Port): String =
      s"$inProcessTeaServerName-${instance.unwrap}-${ledgerApiPort.unwrap}"
  }

  /** Config of the pekko projection in the internal traffic enforcement component
    *
    * The projection restart settings apply to handler failures and non-retryable ingestion stream
    * failures. Retryable Ledger API errors (e.g. UNAVAILABLE while the Ledger API server is not up
    * yet) are retried within the ingestion stream with an exponential backoff (100ms up to 5s, not
    * configurable here). These retries never give up and don't count towards
    * `projectionMaxRestarts`: they are logged at INFO, and at WARN from the 66th consecutive
    * attempt on.
    *
    * @param maxHandlerRetries
    *   max number of retries on projection handler failure
    * @param handlerRetryDelay
    *   delay between restarts of retrying a failed event processing
    * @param minProjectionRestartBackoff
    *   minimum amount of time before retrying the projection on failure
    * @param maxProjectionRestartBackoff
    *   maximum amount of time before between retries of the projection on failure
    * @param projectionMaxRestarts
    *   max number of consecutive projection restarts before the projection gives up. The count
    *   resets once a restarted projection runs for longer than `minProjectionRestartBackoff`
    *   without failing. When the limit is reached, the projection stops processing events, so the
    *   traffic balances it maintains are no longer updated until the participant is restarted.
    * @param initialCompletionOffsetBeginExclusive
    *   the first ever time the TEA is started it will have no prior offset to start its completion
    *   stream from. By default, it will start at ledgerEnd. Use this config to override the offset
    *   at which it will start.
    */
  final case class ProjectionConfig(
      maxHandlerRetries: PositiveInt = PositiveInt.MaxValue,
      handlerRetryDelay: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
      minProjectionRestartBackoff: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
      maxProjectionRestartBackoff: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(30),
      projectionRestartRandomFactor: Double = 0.2d,
      projectionMaxRestarts: Int = Int.MaxValue,
      initialCompletionOffsetBeginExclusive: Option[Long] = None,
  )
}

/** How the traffic enforcement server ingests traffic top-ups. */
sealed trait TopUpConfig extends Product with Serializable

object TopUpConfig {

  /** No top-ups are ingested. Accounts can only be credited through the TEA API. */
  case object Disabled extends TopUpConfig {
    lazy implicit val configReader: ConfigReader[Disabled.type] = deriveReader
    lazy implicit val configWriter: ConfigWriter[Disabled.type] = deriveWriter
  }

  /** Top-ups are ingested from exercises of the `PublishCredit` choice of the `TrafficCreditIssuer`
    * interface in the `canton-traffic-credit` Daml package. The participant uploads that package
    * before it starts ingesting top-ups and vets it on every synchronizer it connects to. With
    * `parameters.admin-workflow.auto-load-dar` disabled, the operator has to upload and vet it
    * instead.
    *
    * @param paymasters
    *   parties whose credits are trusted: only credits published with the authority of one of these
    *   parties, as the paymaster of the issuing contract, are ingested. The parties must be hosted
    *   on this participant.
    * @param initialOffsetBeginExclusive
    *   the first ever time the TEA is started it will have no prior offset to start its top-up
    *   stream from. By default, it will start from the current participant pruned up to offset. Use
    *   this config to override the offset at which it will start.
    */
  final case class DamlCredit(
      paymasters: NonEmpty[Set[PartyId]],
      initialOffsetBeginExclusive: Option[Long] = None,
  ) extends TopUpConfig

  object DamlCredit {
    lazy implicit val configReader: ConfigReader[DamlCredit] = deriveReader
    lazy implicit val configWriter: ConfigWriter[DamlCredit] = deriveWriter
  }

  lazy implicit val configReader: ConfigReader[TopUpConfig] = deriveReader
  lazy implicit val configWriter: ConfigWriter[TopUpConfig] = deriveWriter
}
