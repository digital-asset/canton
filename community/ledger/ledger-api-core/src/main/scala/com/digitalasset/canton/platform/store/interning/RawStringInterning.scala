// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.google.common.annotations.VisibleForTesting

/** @param map
  *   Maps strings to their internal ID. All IDs are non-negative.
  * @param lastId
  *   The last ID in use. Must be non-negative.
  */
private[interning] final case class RawStringInterning @VisibleForTesting private[interning] (
    map: Map[String, Int],
    idMap: Map[Int, String],
    lastId: Int,
)

private[interning] object RawStringInterning {

  def from(
      entries: Iterable[(Int, String)],
      rawStringInterning: RawStringInterning = RawStringInterning(Map.empty, Map.empty, 0),
  ): RawStringInterning =
    if (entries.isEmpty) rawStringInterning
    else {
      val lastId = entries.view.foldLeft(rawStringInterning.lastId) { (lastId, entry) =>
        val (entryId, entryString) = entry
        if (entryId < 0)
          throw new IllegalArgumentException(
            s"String interning IDs must be non-negative, but $entryString has $entryId."
          )
        Math.max(lastId, entryId)
      }
      RawStringInterning(
        map = rawStringInterning.map ++ entries.view.map(_.swap),
        idMap = rawStringInterning.idMap ++ entries,
        lastId = lastId,
      )
    }

  def newEntries(
      distinctRawStrings: Iterable[String],
      rawStringInterning: RawStringInterning,
  ): Vector[(Int, String)] =
    distinctRawStrings.view
      .filterNot(rawStringInterning.map.contains)
      .zipWithIndex
      .map { case (string, index) =>
        val id = index + 1 + rawStringInterning.lastId
        if (id < 0)
          throw new ArithmeticException(
            s"String interning overflow: too many strings interned. Id = $id"
          )
        (id, string)
      }
      .toVector

  def resetTo(
      lastPersistedStringInterningId: Int,
      rawStringInterning: RawStringInterning,
  ): RawStringInterning =
    if (lastPersistedStringInterningId < 0) {
      throw new IllegalArgumentException(
        s"String interning IDs must be non-negative. Got: $lastPersistedStringInterningId"
      )
    } else if (lastPersistedStringInterningId < rawStringInterning.lastId) {
      val idsToBeRemoved = lastPersistedStringInterningId + 1 to rawStringInterning.lastId
      val stringsToBeRemoved = idsToBeRemoved.map(rawStringInterning.idMap)

      RawStringInterning(
        map = rawStringInterning.map.removedAll(stringsToBeRemoved),
        idMap = rawStringInterning.idMap.removedAll(idsToBeRemoved),
        lastId = lastPersistedStringInterningId,
      )
    } else rawStringInterning
}
