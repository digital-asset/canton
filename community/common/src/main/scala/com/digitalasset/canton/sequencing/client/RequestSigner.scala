// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait RequestSigner {
  def signRequest[A <: ProtocolVersionedMemoizedEvidence](
      request: A,
      hashPurpose: HashPurpose,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, SignedContent[A]]
}

object RequestSigner {
  def apply(topologyClient: DomainSyncCryptoClient): RequestSigner = new RequestSigner {
    override def signRequest[A <: ProtocolVersionedMemoizedEvidence](
        request: A,
        hashPurpose: HashPurpose,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, SignedContent[A]] = {
      val snapshot = topologyClient.headSnapshot
      SignedContent
        .create(
          topologyClient.pureCrypto,
          snapshot,
          request,
          Some(snapshot.ipsSnapshot.timestamp),
          hashPurpose,
        )
        .leftMap(_.toString)
    }
  }
}
