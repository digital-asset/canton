// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.config.PemFile
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ValidatedPemCertificateTest extends AnyWordSpec with Matchers with EitherValues {

  private lazy val certChainFile = PemFile(
    ExistingFile.tryCreate("community/app/src/test/resources/tls/public-api.crt")
  )

  "Validate input upon creation" in {
    val validBs = BinaryFileUtil.readByteStringFromFile(certChainFile.pemFile.unwrap.getPath).value
    val invalidBs1 = ByteString.copyFromUtf8("not a PEM certificate")
    val invalidBs2 = ByteString.copyFromUtf8("-----BEGIN CHOPCHOP----- SGVsbG8gV29ybGQ= -----END")

    ValidatedPemCertificate.create(validBs).value shouldBe a[ValidatedPemCertificate]
    ValidatedPemCertificate.create(invalidBs1).left.value should include("Unable to parse")
    ValidatedPemCertificate.create(invalidBs2).left.value should include("Unable to parse")
  }
}
