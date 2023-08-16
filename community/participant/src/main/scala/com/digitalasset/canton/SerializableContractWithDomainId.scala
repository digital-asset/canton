// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.either.*
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.ProtocolVersion

import java.util.Base64

private[canton] final case class SerializableContractWithDomainId(
    domainId: DomainId,
    contract: SerializableContract,
) {

  import SerializableContractWithDomainId.{Delimiter, encoder}

  def encode(protocolVersion: ProtocolVersion): String = {
    val byteStr = contract.toByteString(protocolVersion)
    val encoded = encoder.encodeToString(byteStr.toByteArray)
    val domain = domainId.filterString
    s"$domain$Delimiter$encoded\n"
  }
}

object SerializableContractWithDomainId {
  private val Delimiter = ":::"
  private val decoder = java.util.Base64.getDecoder
  private val encoder: Base64.Encoder = java.util.Base64.getEncoder

  def decode(line: String, lineNumber: Int): Either[String, SerializableContractWithDomainId] =
    line.split(Delimiter).toList match {
      case domainId :: contractByteString :: Nil =>
        for {
          domainId <- DomainId.fromString(domainId)
          contract <- SerializableContract
            .fromByteArray(decoder.decode(contractByteString))
            .leftMap(err => s"Failed parsing disclosed contract: $err")
        } yield SerializableContractWithDomainId(domainId, contract)
      case line => Either.left(s"Failed parsing line $lineNumber: $line ")
    }
}
