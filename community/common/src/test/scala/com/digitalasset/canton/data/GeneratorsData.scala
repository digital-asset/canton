// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.{Salt, TestHash}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.protocol.ConfirmationPolicy
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsData {
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import org.scalatest.EitherValues.*

  private val tenYears: Duration = Duration.ofDays(365 * 10)

  implicit val cantonTimestampArb: Arbitrary[CantonTimestamp] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds * 1000 * 1000).map(CantonTimestamp.ofEpochMicro)
  )
  implicit val cantonTimestampSecondArb: Arbitrary[CantonTimestampSecond] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds).map(CantonTimestampSecond.ofEpochSecond)
  )

  implicit val merklePathElementArg: Arbitrary[MerklePathElement] = genArbitrary

  implicit val commonMetadataArb: Arbitrary[CommonMetadata] = Arbitrary(
    for {
      confirmationPolicy <- implicitly[Arbitrary[ConfirmationPolicy]].arbitrary
      domainId <- implicitly[Arbitrary[DomainId]].arbitrary

      mediatorRef <- implicitly[Arbitrary[MediatorRef]].arbitrary
      singleMediatorRef <- implicitly[Arbitrary[MediatorRef.Single]].arbitrary

      salt <- implicitly[Arbitrary[Salt]].arbitrary
      uuid <- Gen.uuid
      protocolVersion <- representativeProtocolVersionGen(CommonMetadata)

      updatedMediatorRef =
        if (CommonMetadata.shouldHaveSingleMediator(protocolVersion)) singleMediatorRef
        else mediatorRef

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps, protocolVersion)(
        confirmationPolicy,
        domainId,
        updatedMediatorRef,
        salt,
        uuid,
      )
      .value
  )
}
