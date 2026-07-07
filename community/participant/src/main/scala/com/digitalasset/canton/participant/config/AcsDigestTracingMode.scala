// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

/** Setting for how the ACS digest tracing is conducted.
  */
sealed trait AcsDigestTracingMode extends Product with Serializable

object AcsDigestTracingMode {

  /** No tracing
    */
  case object Disabled extends AcsDigestTracingMode

  /** Incremental tracing only tracks the changes applied to the digest persisted as
    * AcsDigestUpdate. In other words, only the changes since the last time a digest was persisted,
    * are tracked.
    */
  case object Incremental extends AcsDigestTracingMode

  /** The entire history, if possible, is persisted with each AcsDigestUpdate. Going from full
    * tracing to disabled or incremental tracing, and then back to full tracing, results in
    * incomplete trace data.
    */
  case object Full extends AcsDigestTracingMode
}
