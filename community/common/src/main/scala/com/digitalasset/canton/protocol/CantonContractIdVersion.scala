// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.client.binding.Primitive
import com.daml.lf.data.Bytes
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

object CantonContractIdVersion {
  val versionPrefixBytesSize = 2

  def fromProtocolVersion(protocolVersion: ProtocolVersion): CantonContractIdVersion =
    if (protocolVersion >= ProtocolVersion.v4) AuthenticatedContractIdVersion
    else NonAuthenticatedContractIdVersion

  def ensureCantonContractId(
      contractId: LfContractId
  ): Either[MalformedContractId, CantonContractIdVersion] =
    contractId match {
      case LfContractId.V1(_discriminator, suffix) =>
        for {
          versionedContractId <- suffix match {
            case s if s.startsWith(AuthenticatedContractIdVersion.versionPrefixBytes) =>
              Right(AuthenticatedContractIdVersion)
            case s if s.startsWith(NonAuthenticatedContractIdVersion.versionPrefixBytes) =>
              Right(NonAuthenticatedContractIdVersion)
            case invalidSuffix =>
              Left(
                MalformedContractId(
                  contractId.toString,
                  s"Suffix ${invalidSuffix.toHexString} does not start with one of the supported prefixes: ${AuthenticatedContractIdVersion.versionPrefixBytes} or ${NonAuthenticatedContractIdVersion.versionPrefixBytes}",
                )
              )
          }

          unprefixedSuffix = suffix.slice(versionPrefixBytesSize, suffix.length)

          _ <- Hash
            .fromByteString(unprefixedSuffix.toByteString)
            .leftMap(err => MalformedContractId(contractId.toString, err.message))
        } yield versionedContractId
    }
}

sealed trait CantonContractIdVersion extends Serializable with Product {
  require(
    versionPrefixBytes.length == CantonContractIdVersion.versionPrefixBytesSize,
    s"Version prefix of size ${versionPrefixBytes.length} should have size ${CantonContractIdVersion.versionPrefixBytesSize}",
  )

  def versionPrefixBytes: Bytes

  def fromDiscriminator(discriminator: LfHash, unicum: Unicum): LfContractId.V1 =
    LfContractId.V1(discriminator, unicum.toContractIdSuffix(this))
}

case object NonAuthenticatedContractIdVersion extends CantonContractIdVersion {
  // The prefix for the suffix of non-authenticated (legacy) Canton contract IDs
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x00.toByte))
}

case object AuthenticatedContractIdVersion extends CantonContractIdVersion {
  // The prefix for the suffix of Canton contract IDs for contracts that can be authenticated (created in Protocol V4+)
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x01.toByte))
}

object ContractIdSyntax {
  implicit class ScalaCodegenContractIdSyntax[T](contractId: ApiTypes.ContractId) {
    def toLf: LfContractId = LfContractId.assertFromString(contractId.toString)
  }

  implicit class LfContractIdSyntax(private val contractId: LfContractId) extends AnyVal {
    def toProtoPrimitive: String = contractId.coid

    /** An [[LfContractId]] consists of
      * - a version (1 byte)
      * - a discriminator (32 bytes)
      * - a suffix (at most 94 bytes)
      * Thoses 1 + 32 + 94 = 127 bytes are base-16 encoded, so this makes 254 chars at most.
      * See https://github.com/digital-asset/daml/blob/main/daml-lf/spec/contract-id.rst
      */
    def toLengthLimitedString: String255 = checked(String255.tryCreate(contractId.coid))
    def encodeDeterministically: ByteString = ByteString.copyFromUtf8(toProtoPrimitive)

    /** Converts an [[LfContractId]] into a contract ID bound to a template usable with the Scala codegen API.
      * `Unchecked` means that we do not check that the contract ID actually refers to a contract of
      * the template `Tpl`.
      */
    def toPrimUnchecked[Tpl]: Primitive.ContractId[Tpl] =
      Primitive.ContractId.apply(contractId.coid)
  }

  implicit val orderingLfContractId: Ordering[LfContractId] =
    Ordering.by[LfContractId, String](_.coid)
}

case class MalformedContractId(id: String, message: String) {
  override def toString: String =
    s"malformed contract id '$id'" + (if (message.nonEmpty) s". $message" else "")
}
