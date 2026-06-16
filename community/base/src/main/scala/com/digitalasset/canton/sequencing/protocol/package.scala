// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

package object protocol {

  /** A [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]] whose batch has already been
    * decompressed into a [[com.digitalasset.canton.sequencing.protocol.Batch]] of envelopes (as
    * opposed to a still-compressed
    * [[com.digitalasset.canton.sequencing.protocol.CompressedBatch]]).
    */
  type DecompressedSequencedEvent[+Env <: Envelope[?]] = SequencedEvent[Batch[Env]]
}
