// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config

/** Configuration of a DB lock
  *
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between one health check completed and the next to
  *   start.
  * @param healthCheckTimeout
  *   Timeout for running a health check in seconds granularity.
  */
final case class DbLockConfig(
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    healthCheckTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
)

object DbLockConfig {

  /** For locks to be supported we must be using an [[DbConfig]] with it set to Postgres. */
  private[canton] def isSupportedConfig(config: StorageConfig): Boolean =
    PartialFunction.cond(config) { case _: DbConfig.Postgres =>
      true
    }

  // we need to preallocate a range of lock counters for concurrently running sequencer writers
  // this actually sets the limit of the number of concurrent sequencers that we allow
  val MAX_SEQUENCER_WRITERS_AVAILABLE = 32

}

/** Configuration of a DB-locked connection, i.e., a database connection with an associated DB lock.
  *
  * @param passiveCheckPeriod
  *   How long to wait between trying to become active.
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between one health check completed and the next to
  *   start.
  * @param healthCheckTimeout
  *   Timeout for running a health check in seconds granularity.
  * @param connectionTimeout
  *   Timeout for requesting a new connection from the underlying database driver in seconds
  *   granularity.
  * @param keepAliveIdle
  *   TCP keep-alive idle time, i.e., how long to wait until sending a keep-alive message when idle.
  * @param keepAliveInterval
  *   TCP keep-alive interval, i.e., how long to wait until resending an unanswered keep-alive
  *   message.
  * @param keepAliveCount
  *   TCP keep-alive count, i.e., how many unanswered keep-alive messages required to consider the
  *   connection lost.
  * @param clientConnectionCheckInterval
  *   Interval for client connection checks. Corresponds to the Postgres
  *   `client_connection_check_interval` configuration parameter (Postgres >= 14 only). Millisecond
  *   granularity is the lowest supported precision. `None` leaves the server default unchanged
  *   whereas a Some(0) explicitly disables the check. On macOS with Postgres 14, this must be
  *   disabled, since in Postgres 14, this setting requires the non-standard `POLLRDHUP` extension
  *   to the `poll` system call, which is only available on Linux. See
  *   [[https://www.postgresql.org/docs/14/runtime-config-connection.html]] for details.
  * @param initialAcquisitionMaxRetries
  *   Maximum number of retries when trying to acquire the lock for the first time before trying to
  *   acquire the lock in a background task.
  * @param initialAcquisitionInterval
  *   Retry intervals during the initial attempts to acquire the lock.
  * @param lock
  *   Configuration of the DB locks used by the pool.
  */
final case class DbLockedConnectionConfig(
    passiveCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    healthCheckTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    connectionTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
    keepAliveIdle: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
    keepAliveInterval: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
    keepAliveCount: Int = 5,
    clientConnectionCheckInterval: Option[config.NonNegativeFiniteDuration] = Some(
      config.NonNegativeFiniteDuration.ofSeconds(5)
    ),
    initialAcquisitionMaxRetries: Int = 5,
    initialAcquisitionInterval: PositiveFiniteDuration = PositiveFiniteDuration.ofMillis(200),
    lock: DbLockConfig = DbLockConfig(),
)

/** Configuration for the connection pool using DB locks.
  *
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between health checks of the connection pool.
  * @param connection
  *   Configuration of the DB locked connection used by the pool.
  * @param activeTimeout
  *   Time to wait until the first connection in the pool becomes active during failover.
  * @param initialLockedConnectionTimeout
  *   Time to wait until the initial connection is either active or passive (external sequencer does
  *   not use this)
  * @param initialMustRemainActiveConnectionTimeout
  *   If using an external sequencer we want to make sure it is always active, unless it is closing.
  *   This timeout is the initial time to wait before first becoming active, otherwise it is
  *   shutdown.
  */
final case class DbLockedConnectionPoolConfig(
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    connection: DbLockedConnectionConfig = DbLockedConnectionConfig(),
    activeTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    initialLockedConnectionTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    initialMustRemainActiveConnectionTimeout: PositiveFiniteDuration =
      PositiveFiniteDuration.ofMinutes(1),
)
