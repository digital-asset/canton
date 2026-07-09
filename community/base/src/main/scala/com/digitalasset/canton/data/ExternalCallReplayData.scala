// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.data.ExternalCallPayloadDescription.{byteCount, byteSize}
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

/** External-call replay data: the single recorded output per semantic call, indexed by
  * [[ExternalCallKey]].
  *
  * Constructed only via [[ExternalCallReplayData.fromResults]], which fails if a key is recorded
  * with more than one distinct output. A value of this type therefore always maps a key to the one
  * unambiguous output to replay, which is why [[outputFor]] returns a plain `Option`.
  */
final case class ExternalCallReplayData private (
    outputsByKey: Map[ExternalCallKey, Bytes]
) {
  def outputFor(key: ExternalCallKey): Option[Bytes] = outputsByKey.get(key)
}

object ExternalCallReplayData {
  val empty: ExternalCallReplayData = ExternalCallReplayData(Map.empty)

  /** Indexes the recorded results by their semantic [[ExternalCallKey]]. Identical results recorded
    * in several occurrences collapse to a single entry; a key recorded with more than one distinct
    * output yields a `Left` describing the conflict by payload size only, never the payload bytes.
    */
  def fromResults(
      results: Iterable[ExternalCallResult]
  ): Either[String, ExternalCallReplayData] =
    MapsUtil
      .toNonConflictingMap(results.map(result => ExternalCallKey.fromResult(result) -> result.output))
      .leftMap(conflictMessage)
      .map(ExternalCallReplayData(_))

  private def conflictMessage(conflicts: Map[ExternalCallKey, Set[Bytes]]): String = {
    val details = conflicts.toSeq
      .sortBy { case (key, _) => key }
      .map { case (key, outputs) =>
        s"$key with outputs ${outputs.toSeq.sortBy(byteCount).map(byteSize).mkString("[", ", ", "]")}"
      }
    s"externalCallResults records conflicting outputs for the same external call: ${details.mkString("; ")}"
  }
}
