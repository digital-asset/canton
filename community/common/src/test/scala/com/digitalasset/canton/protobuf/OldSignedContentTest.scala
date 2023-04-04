// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protobuf

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{Signature, v0 as cryptoproto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class OldSignedContentTest extends AnyWordSpec with BaseTest {
  private val signature1 =
    cryptoproto.Signature(
      format = cryptoproto.SignatureFormat.RawSignatureFormat,
      signature = ByteString.EMPTY,
      signedBy = "signer1",
    )

  private val signature2 = signature1.copy(signedBy = "signer2")

  private val message =
    v0.SignedContent(
      Some(ByteString.EMPTY),
      Seq(signature1, signature2),
      timestampOfSigningKey = None,
    )

  def oldSignedContentParserMethod[A <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[A],
      signedValueP: OldSignedContent,
  ): ParsingResult[SignedContent[A]] =
    signedValueP match {
      case OldSignedContent(content, maybeSignatureP, timestampOfSigningKey) =>
        for {
          contentB <- ProtoConverter.required("content", content)
          content <- contentDeserializer(contentB)
          signature <- ProtoConverter.parseRequired(
            Signature.fromProtoV0,
            "signature",
            maybeSignatureP,
          )
          ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
        } yield SignedContent(content, signature, ts, ProtocolVersion.v3)
    }

  private def parseOld(
      message: OldSignedContent
  ): ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] =
    oldSignedContentParserMethod(
      _ => Either.right(BytestringWithCryptographicEvidence(ByteString.EMPTY)),
      message,
    )

  private def parseNew(
      message: v0.SignedContent
  ): ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] =
    SignedContent
      .fromProtoV0(message)
      .flatMap(
        _.deserializeContent(_ => Right(BytestringWithCryptographicEvidence(ByteString.EMPTY)))
      )

  "Older SignedContent serializer" should {
    "act the same as new and take the last element" in {
      val newerMessageParsedByOldSystem =
        ProtoConverter.protoParser(OldSignedContent.parseFrom)(message.toByteString).value

      val parsedOldMessage = parseOld(newerMessageParsedByOldSystem).value
      val parsedNewMessage = parseNew(message).value
      parsedOldMessage.signature shouldBe parsedNewMessage.signature
    }
  }
}
