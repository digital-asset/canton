// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.lf.data.Bytes
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString

object ContractId {

  // The prefix for the suffix of Canton contract IDs
  val suffixPrefix: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x00.toByte))
  val suffixPrefixHex: String = suffixPrefix.toHexString

  private def withoutPrefix(suffix: Bytes): Bytes = suffix.slice(suffixPrefix.length, suffix.length)

  def ensureCantonContractId(contractId: LfContractId): Either[MalformedContractId, Unit] = {
    contractId match {
      case LfContractId.V1(_discriminator, suffix) =>
        for {
          _ <- Either.cond(
            suffix.startsWith(suffixPrefix),
            (),
            MalformedContractId(
              contractId.toString,
              s"Suffix ${suffix.toHexString} does not start with prefix ${suffixPrefix.toHexString}",
            ),
          )
          _ <- Hash
            .fromByteString(withoutPrefix(suffix).toByteString)
            .leftMap(err => MalformedContractId(contractId.toString, err.message))
        } yield ()
    }
  }

  def fromDiscriminator(discriminator: LfHash, unicum: Unicum): LfContractId.V1 =
    LfContractId.V1(discriminator, unicum.toContractIdSuffix)
}

object ContractIdSyntax {
  implicit class ScalaCodegenContractIdSyntax[T](contractId: ApiTypes.ContractId) {
    def toLf: LfContractId = LfContractId.assertFromString(contractId.toString)
  }

  implicit class LfContractIdSyntax(private val contractId: LfContractId) extends AnyVal {
    def toProtoPrimitive: String = contractId.coid
    def encodeDeterministically: ByteString = ByteString.copyFromUtf8(toProtoPrimitive)
  }

  implicit class LfContractIdObjectSyntax(private val lfContractId: LfContractId.type)
      extends AnyVal {
    def fromProtoPrimitive(contractIdP: String): ParsingResult[LfContractId] =
      LfContractId.fromString(contractIdP).leftMap(err => StringConversionError(err))
  }

  implicit val orderingLfContractId: Ordering[LfContractId] =
    Ordering.by[LfContractId, String](_.coid)
}

case class MalformedContractId(id: String, message: String) {
  override def toString: String =
    s"malformed contract id '$id'" + (if (message.nonEmpty) s". $message" else "")
}
