// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.bifunctor.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ResourceUtil
import com.google.protobuf.ByteString

import java.security.cert.CertificateFactory

final case class ValidatedPemCertificate private (pem: ByteString) {
  def toProtoPrimitive: ByteString = pem
}

object ValidatedPemCertificate {
  def create(pem: ByteString): Either[String, ValidatedPemCertificate] = {
    lazy val errorMessage = s"Unable to parse $pem as a valid certificate"

    for {
      certificates <- ResourceUtil
        .withResourceEither(pem.newInput()) { inputStream =>
          CertificateFactory.getInstance("X.509").generateCertificates(inputStream)
        }
        .leftMap(_ => errorMessage)

      _ <- Either.cond(!certificates.isEmpty, (), errorMessage)

    } yield ValidatedPemCertificate(pem)
  }

  def fromProtoPrimitive(pem: ByteString): ParsingResult[ValidatedPemCertificate] =
    create(pem).leftMap(ProtoDeserializationError.OtherError(_))

}
