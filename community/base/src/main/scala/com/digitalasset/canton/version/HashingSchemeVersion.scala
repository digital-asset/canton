// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.collection.immutable.{SortedMap, SortedSet}

sealed abstract class HashingSchemeVersion(val index: Int) {
  def toProtoV30: v30.ExternalAuthorization.HashingSchemeVersion =
    v30.ExternalAuthorization.HashingSchemeVersion.fromValue(index)
}

object HashingSchemeVersion {
  final case object V2 extends HashingSchemeVersion(2)
  implicit val hashingSchemeVersionOrdering: Ordering[HashingSchemeVersion] =
    Ordering.by(_.index)

  private val ProtocolVersionToHashingVersion =
    SortedMap[ProtocolVersion, NonEmpty[SortedSet[HashingSchemeVersion]]](
      ProtocolVersion.v34 -> NonEmpty.mk(SortedSet, V2),
      ProtocolVersion.dev -> NonEmpty.mk(SortedSet, V2),
    )

  def minProtocolVersionForHSV(version: HashingSchemeVersion): Option[ProtocolVersion] =
    ProtocolVersionToHashingVersion.iterator.collectFirst {
      case (pv, isVersions) if isVersions.contains(version) => pv
    }

  def getHashingSchemeVersionsForProtocolVersion(
      protocolVersion: ProtocolVersion
  ): NonEmpty[SortedSet[HashingSchemeVersion]] = {
    assert(
      protocolVersion >= ProtocolVersion.v34,
      s"Canton only supports external signing from ProtocolVersions >= ${ProtocolVersion.v34}",
    )
    ProtocolVersionToHashingVersion(protocolVersion)
  }

  def fromProtoV30(
      version: v30.ExternalAuthorization.HashingSchemeVersion
  ): ParsingResult[HashingSchemeVersion] = version match {
    case v30.ExternalAuthorization.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2 => Right(V2)
    case v30.ExternalAuthorization.HashingSchemeVersion.HASHING_SCHEME_VERSION_UNSPECIFIED =>
      Left(FieldNotSet("hashing_scheme_version"))
    case v30.ExternalAuthorization.HashingSchemeVersion.Unrecognized(unrecognizedValue) =>
      Left(UnrecognizedEnum("hashing_scheme_version", unrecognizedValue))
  }
}
