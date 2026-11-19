// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.daml.testing.utils.{PekkoBeforeAndAfterAll, TestResourceContext}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.apiserver.LedgerApiService
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

final class TlsCertificateRevocationCheckingSpec
    extends AsyncWordSpec
    with MockitoSugar
    with PekkoBeforeAndAfterAll
    with TestResourceContext
    with OcspResponderFixture
    with BaseTest {

  classOf[LedgerApiService.type].getSimpleName when {
    "certificate revocation checking is enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(
          loggerFactory,
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientCrt),
          Some(clientKey),
          certRevocationChecking = true,
        )
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections with revoked certificates" in {
        TlsFixture(
          loggerFactory,
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientRevokedCrt),
          Some(clientRevokedKey),
          certRevocationChecking = true,
        )
          .makeARequest()
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }

    // Revocation checking is scoped to the SSL context, so these run in the same JVM after the
    // cases above and must not be affected by them.
    "certificate revocation checking is not enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(
          loggerFactory,
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientCrt),
          Some(clientKey),
          certRevocationChecking = false,
        )
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections with revoked certificates" in {
        TlsFixture(
          loggerFactory,
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientRevokedCrt),
          Some(clientRevokedKey),
          certRevocationChecking = false,
        )
          .makeARequest()
          .map(_ => succeed)
      }
    }
  }
}
