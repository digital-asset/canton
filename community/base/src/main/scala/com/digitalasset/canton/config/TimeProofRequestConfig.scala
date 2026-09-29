// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.option.*
import com.digitalasset.canton.admin.time.v30
import com.digitalasset.canton.config
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Various config values for the time proof request sender
  *
  * @param requestTimeout
  *   max sequencing time on the time proof request relative to the local clock. If the local clock
  *   and the sequencer clock differ too much, then the node will never be able to learn the
  *   sequencers time.
  * @param initialRetryDelay
  *   The initial retry delay if the request to send a sequenced event fails
  * @param maxRetryDelay
  *   The max retry delay if the request to send a sequenced event fails
  * @param maxSequencingDelay
  *   If our request for a sequenced event was successful, how long should we wait to observe it
  *   from the sequencer before starting a new request.
  */
final case class TimeProofRequestConfig(
    initialRetryDelay: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultInitialRetryDelay,
    maxRetryDelay: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultMaxRetryDelay,
    maxSequencingDelay: NonNegativeFiniteDuration =
      TimeProofRequestConfig.defaultMaxSequencingDelay,
    requestTimeout: NonNegativeFiniteDuration = TimeProofRequestConfig.defaultRequestTimeout,
) extends PrettyPrinting {
  private[config] def toProtoV30: v30.TimeProofRequestConfig = v30.TimeProofRequestConfig(
    initialRetryDelay = initialRetryDelay.toProtoPrimitive.some,
    maxRetryDelay = maxRetryDelay.toProtoPrimitive.some,
    maxSequencingDelay = maxSequencingDelay.toProtoPrimitive.some,
    requestTimeout = requestTimeout.toProtoPrimitive.some,
  )
  override protected def pretty: Pretty[TimeProofRequestConfig] = prettyOfClass(
    paramIfNotDefault(
      "initialRetryDelay",
      _.initialRetryDelay,
      TimeProofRequestConfig.defaultInitialRetryDelay,
    ),
    paramIfNotDefault(
      "maxRetryDelay",
      _.maxRetryDelay,
      TimeProofRequestConfig.defaultMaxRetryDelay,
    ),
    paramIfNotDefault(
      "maxSequencingDelay",
      _.maxSequencingDelay,
      TimeProofRequestConfig.defaultMaxSequencingDelay,
    ),
    paramIfNotDefault(
      "requestTimeout",
      _.requestTimeout,
      TimeProofRequestConfig.defaultRequestTimeout,
    ),
  )

}

object TimeProofRequestConfig {

  private val defaultRequestTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(2)

  private val defaultInitialRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(200)

  private val defaultMaxRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)

  private val defaultMaxSequencingDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)

  private[config] def fromProtoV30(
      configP: v30.TimeProofRequestConfig
  ): ParsingResult[TimeProofRequestConfig] = {
    def parse(
        name: String,
        value: Option[com.google.protobuf.duration.Duration],
    ): ParsingResult[config.NonNegativeFiniteDuration] =
      config.NonNegativeFiniteDuration.fromProtoPrimitiveO(name)(value)

    for {
      initialRetryDelay <- parse("initialRetryDelay", configP.initialRetryDelay)
      maxRetryDelay <- parse("maxRetryDelay", configP.maxRetryDelay)
      maxSequencingDelay <- parse("maxSequencingDelay", configP.maxSequencingDelay)
      // backwards compatible parsing of parameter introduced with 3.6.1+
      requestTimeout <- configP.requestTimeout
        .map(config.NonNegativeFiniteDuration.fromProtoPrimitive("requestTimeout"))
        .getOrElse(Right(defaultRequestTimeout))
    } yield TimeProofRequestConfig(
      initialRetryDelay = initialRetryDelay,
      maxRetryDelay = maxRetryDelay,
      maxSequencingDelay = maxSequencingDelay,
      requestTimeout = requestTimeout,
    )
  }
}
