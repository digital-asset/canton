// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorageSingle, Storage}
import com.typesafe.config.{Config, ConfigFactory}

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
  *   submission will be rejected. When disabled, the participant does not contact the traffic
  *   service on the submission path at all. Disabled by default.
  * @param rejectMultiPartySubmissions
  *   Whether to reject submissions whose actAs has more than one party. TEA accounts are bound to a
  *   single party, so by default such submissions bypass traffic enforcement entirely (validation
  *   is skipped and an informational message is logged). When enabled, such submissions are
  *   rejected instead. Disabled by default.
  * @param allowSubmissionsOnDegradation
  *   Whether to allow a submission to proceed without a balance check when the balance cannot be
  *   determined, for example due to a database outage. The submission is still charged, so an
  *   account that didn't have enough traffic balance might end up with a negative balance until it
  *   is topped up. Doesn't apply when the traffic service refuses the request. A bypassed check is
  *   logged at WARN. Disabled by default, so a failed lookup fails the submission.
  * @param trafficEnforcementServer
  *   The configuration for the connection to the traffic server. Currently, only the internal,
  *   in-process server variant is supported.
  */
final case class TrafficEnforcementConfig(
    enabled: Boolean = false,
    enforceCostOnSubmissions: Boolean = false,
    rejectMultiPartySubmissions: Boolean = false,
    allowSubmissionsOnDegradation: Boolean = false,
    trafficEnforcementServer: TrafficEnforcementServerConfig =
      TrafficEnforcementServerConfig.Internal(),
)

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
    *   query still leaves room for a retry.
    */
  final case class Internal(
      inProcessTeaServerName: String = "TeaGrpcInProcServer",
      projection: ProjectionConfig = ProjectionConfig(),
      databaseQueryTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
      accountLookupTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(20),
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

    def pekkoConfig(storage: Storage): Config = storage match {
      case storage: DbStorageSingle =>
        storage.profile match {
          case _: Profile.H2 =>
            // H2 creates upper case tables but pekko expects lower case by default
            // this configures pekko to look for upper case ones
            ConfigFactory.parseString(
              """pekko.projection.slick.offset-store {
                |  use-lowercase-schema = false
                |  table = "PEKKO_PROJECTION_OFFSET_STORE"
                |  management-table = "PEKKO_PROJECTION_MANAGEMENT"
                |}""".stripMargin
            )
          case _ => ConfigFactory.empty()
        }
      case _ => ConfigFactory.empty()
    }
  }

  /** Config of the pekko projection in the internal traffic enforcement component
    * @param maxHandlerRetries
    *   max number of retries on projection handler failure
    * @param handlerRetryDelay
    *   delay between restarts of retrying a failed event processing
    * @param minProjectionRestartBackoff
    *   minimum amount of time before retrying the projection on failure
    * @param maxProjectionRestartBackoff
    *   maximum amount of time before between retries of the projection on failure
    * @param initialCompletionOffsetBeginExclusive
    *   the first ever time the TEA is started it will have no prior offset to start its completion
    *   stream from. By default it will start at ledgerEnd. Use this config to override the offset
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
