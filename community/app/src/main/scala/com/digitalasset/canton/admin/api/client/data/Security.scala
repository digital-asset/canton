// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*

/** A tagged class used to return exported private keys */
case class SerializedPrivateKey(payload: String)

case class CertificateResult(certificateId: CertificateId, x509Pem: String)

object CertificateResult {
  def fromPem(x509Pem: String): Either[X509CertificateError, CertificateResult] = {
    for {
      pem <- X509CertificatePem
        .fromString(x509Pem)
        .leftMap(err => X509CertificateError.DecodingError(err))
      cert <- X509Certificate.fromPem(pem)
    } yield CertificateResult(cert.id, x509Pem)
  }
}
