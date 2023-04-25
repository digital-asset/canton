// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig

/** Client configured options for how to connect to a sequencer
  *
  * @param eventInboxSize The size of the inbox queue used to store received events. Must be at least one.
  *                       Events in the inbox are processed in parallel.
  *                       A larger inbox may result in higher throughput at the price of higher memory consumption,
  *                       larger database queries, and longer crash recovery.
  * @param startupConnectionRetryDelay Initial delay before we attempt to establish an initial connection
  * @param initialConnectionRetryDelay Initial delay before a reconnect attempt
  * @param warnDisconnectDelay Consider sequencer to be degraded after delay
  * @param maxConnectionRetryDelay Maximum delay before a reconnect attempt
  * @param handshakeRetryAttempts How many attempts should we make to get a handshake response
  * @param handshakeRetryDelay How long to delay between attempts to fetch a handshake response
  * @param defaultMaxSequencingTimeOffset if no max-sequencing-time is supplied to send, our current time will be offset by this amount
  * @param acknowledgementInterval Controls how frequently the client acknowledges how far it has successfully processed
  *                                to the sequencer which allows the sequencer to remove this data when pruning.
  * @param keepAlive keep alive config used for GRPC sequencers
  * @param authToken configuration settings for the authentication token manager
  * @param optimisticSequencedEventValidation if true, sequenced event signatures will be validated first optimistically
  *                                           and only strict if the optimistic evaluation failed. this means that
  *                                           for a split second, we might still accept an event signed with a key that
  *                                           has just been revoked.
  * @param skipSequencedEventValidation if true, sequenced event validation will be skipped. the default setting is false.
  *                                     this option should only be enabled if a defective validation is blocking processing.
  *                                     therefore, unless you know what you are doing, you shouldn't touch this setting.
  * @param overrideMaxRequestSize overrides the maxRequestSize configured in the dynamic domain parameters. If overrideMaxRequestSize,
  *                               is set, modifying the maxRequestSize won't have any effect.
  */
final case class SequencerClientConfig(
    eventInboxSize: PositiveInt = PositiveInt.tryCreate(100),
    startupConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(1),
    initialConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(10),
    warnDisconnectDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    maxConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
    handshakeRetryAttempts: NonNegativeInt = NonNegativeInt.tryCreate(50),
    handshakeRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    defaultMaxSequencingTimeOffset: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(5),
    acknowledgementInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
    keepAliveClient: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
    authToken: AuthenticationTokenManagerConfig = AuthenticationTokenManagerConfig(),
    // TODO(#10040) remove optimistic validation
    optimisticSequencedEventValidation: Boolean = true,
    skipSequencedEventValidation: Boolean = false,
    overrideMaxRequestSize: Option[NonNegativeInt] = None,
)
