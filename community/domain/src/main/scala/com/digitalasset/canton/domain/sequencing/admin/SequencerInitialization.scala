// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.InstanceName
import com.digitalasset.canton.crypto.store.CryptoPublicStore
import com.digitalasset.canton.crypto.{KeyName, KeyPurpose, PublicKey}
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.domain.sequencing.admin.client.SequencerAdminClient
import com.digitalasset.canton.domain.sequencing.admin.protocol.InitRequest
import com.digitalasset.canton.topology.transaction.{OwnerToKeyMapping, TopologyStateUpdate}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.sequencing.handshake.SequencerHandshake
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

object SequencerInitialization {

  /** Attempt to initialize the sequencer using the provided sequencerInitializer.
    * Currently this expects that the sequencer is available and can process the initialization request successfully.
    * For external sequencers this requires that it is started and ready to accept requests before the domain is initialized.
    * In the future we may want to add retries to allow for the sequencer to start after the domain.
    */
  def attemptInitialization(
      domainName: InstanceName,
      config: SequencerClientConfig,
      sequencerAdminClient: SequencerAdminClient,
      topologyManager: DomainTopologyManager,
      cryptoPublicStore: CryptoPublicStore,
      initRequest: InitRequest,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, PublicKey] =
    for {
      // verify the server version first
      _ <- SequencerHandshake.handshake(
        ProtocolVersion.supportedProtocolsDomain,
        minimumProtocolVersion = None,
        sequencerAdminClient,
        config,
        timeouts,
        loggerFactory,
      )
      response <- sequencerAdminClient.initialize(initRequest)
      mapping = OwnerToKeyMapping(SequencerId(initRequest.domainId), response.publicKey)
      // only attempt to register the key if it isn't yet registered.
      // this is especially useful for the in-process sequencer which shares the same key registry so was registered
      // when the key was generated.
      // it will also be useful if the initialization process fails after this point and the sequencer initialization
      // is attempted more than once.
      exists <- cryptoPublicStore
        .existsPublicKey(response.publicKey.fingerprint, KeyPurpose.Signing)
        .leftMap(_.toString)
      _ = if (!exists)
        cryptoPublicStore.storePublicKey(
          response.publicKey,
          Some(KeyName.tryCreate(s"$domainName-signing")),
        )
      else ()
      _ <- topologyManager
        .authorize(
          TopologyStateUpdate.createAdd(mapping),
          Some(initRequest.domainId.unwrap.namespace.fingerprint),
          force = false,
        )
        .leftMap(_.toString)
    } yield response.publicKey
}
