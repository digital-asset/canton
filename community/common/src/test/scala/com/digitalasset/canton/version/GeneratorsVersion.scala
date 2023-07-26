// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.NonEmpty
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsVersion {
  private val allProtocolVersions: NonEmpty[List[ProtocolVersion]] =
    (ProtocolVersion.unstable ++ ProtocolVersion.supported)

  val protocolVersionGen: Gen[ProtocolVersion] = Gen.oneOf(allProtocolVersions)

  implicit val protocolVersionArb: Arbitrary[ProtocolVersion] = Arbitrary(
    Gen.oneOf(allProtocolVersions)
  )

  def defaultValueGen[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: ProtocolVersion,
      defaultValue: Comp#DefaultValue[T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    arb.arbitrary.map(defaultValue.orValue(_, protocolVersion))

  def defaultValueArb[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: RepresentativeProtocolVersion[Comp],
      defaultValue: Comp#DefaultValue[T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    defaultValueGen(protocolVersion.representative, defaultValue)

  def representativeProtocolVersionGen[ValueClass](
      companion: HasProtocolVersionedWrapperCompanion[ValueClass, _]
  ): Gen[RepresentativeProtocolVersion[companion.type]] =
    Gen.oneOf(companion.supportedProtoVersions.table.values)
}
