// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}

import scala.collection.immutable.SortedMap

/** A validation parameter whose value depends on the protocol version, for example a maximum length
  * limit that validation enforces.
  *
  * List a value only for the versions where it changes. Each value stays in effect until the next
  * listed version. If the protocol version is older than every version listed, there is no value
  * and the validation is skipped.
  *
  * {{{
  * ValidationParam(v37 -> y)                     // v36: no value, v37 and later: y
  * ValidationParam(v36 -> x, v37 -> y)           // v36: x, v37 and later: y
  * ValidationParam(ProtocolVersion.minimum -> x) // every version: x
  * }}}
  */
final case class ValidationParam[A] private (
    private val entries: NonEmpty[SortedMap[ProtocolVersion, A]]
) {

  /** The value listed for `pv`, or for the closest older version if `pv` itself is not listed.
    * `None` if `pv` is older than every listed version.
    */
  def forProtocolVersion(pv: ProtocolVersion): Option[A] =
    entries.rangeTo(pv).lastOption.map(_._2)

  /** The value for the newest listed version.
    */
  def latest: A = entries.last1._2
}

object ValidationParam {

  /** Pass the first version the setting applies to and its value, then one pair for every later
    * version that changes it.
    */
  def apply[A](
      entry: (ProtocolVersion, A),
      moreEntries: (ProtocolVersion, A)*
  ): ValidationParam[A] = {
    val versions = (entry +: moreEntries).map(_._1)
    require(
      versions.distinct.sizeIs == versions.size,
      s"a protocol version may carry at most one value, got $versions",
    )
    // Non-empty by construction: `entry` is always present.
    new ValidationParam(NonEmptyUtil.fromUnsafe(SortedMap.from(entry +: moreEntries)))
  }
}
