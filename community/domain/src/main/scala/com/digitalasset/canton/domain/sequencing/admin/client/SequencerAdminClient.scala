// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.client

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.config.{Password, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.crypto.store.ProtectedKeyStore
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.handshake.SupportsHandshake
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  HttpSequencerConnection,
  SequencerConnection,
}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}

import scala.concurrent.{ExecutionContextExecutor, Future}

/** Interface for performing administrative operations against a sequencer */
trait SequencerAdminClient extends FlagCloseable with SupportsHandshake {

  /** Called during domain initialization with appropriate identities and public keys for the sequencer to initialize itself.
    * If successful the sequencer is expected to return its PublicKey and be immediately ready to accept requests.
    */
  def initialize(request: InitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, InitResponse]
}

object SequencerAdminClient {
  def create(
      connection: SequencerConnection,
      domainParameters: StaticDomainParameters,
      processingTimeout: ProcessingTimeout,
      traceContextPropagation: TracingConfig.Propagation,
      crypto: Crypto,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerAdminClient] =
    connection match {
      case HttpSequencerConnection(urls, certificate) =>
        for {
          keystore <- crypto.javaKeyConverter
            .toJava(crypto.cryptoPrivateStore, crypto.cryptoPublicStore, Password.empty)
            .leftMap(err => s"Failed to convert internal crypto stores into java keystore: $err")
          httpClient <- HttpClient
            .create(certificate, ProtectedKeyStore(keystore, Password.empty), None)(
              processingTimeout,
              loggerFactory,
            )
            .leftMap(err => s"Failed to create http transport for sequencer client: $err")
            .toEitherT[Future]
        } yield new HttpSequencerAdminClient(
          urls.write,
          httpClient,
          processingTimeout,
          loggerFactory,
        )
      case grpc: GrpcSequencerConnection =>
        EitherT.rightT(
          new GrpcSequencerAdminClient(
            grpc,
            domainParameters,
            traceContextPropagation,
            processingTimeout,
            loggerFactory,
          )
        )
    }
}
