// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.protocol.{v30, v31, v32}

/** A small abstraction so we can reuse some deserialization logic in [[SubmissionRequest]] and
  * [[SequencedEvent]]
  */
sealed trait ProtoBatch extends Product with Serializable
final case class ProtoBatchV30(wrapped: v30.CompressedBatch) extends ProtoBatch
final case class ProtoBatchV31(wrapped: v31.CompressedBatch) extends ProtoBatch
final case class ProtoBatchV32(wrapped: v32.CompressedBatch) extends ProtoBatch
