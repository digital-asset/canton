// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.{EitherT, OptionT}
import cats.syntax.foldable.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.crypto.{
  BaseCrypto,
  Crypto,
  EncryptionPublicKey,
  Fingerprint,
  KeyName,
  PublicKey,
  SigningKeyUsage,
  SigningPublicKey,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

final case class GenerateOnboardingTransactions(
    nodeId: UniqueIdentifier,
    member: Member,
    instanceName: InstanceName,
    clock: Clock,
    crypto: Crypto,
    batchingConfig: BatchingConfig,
    topologyConfig: TopologyConfig,
    futureSupervisor: FutureSupervisor,
    processingTimeout: ProcessingTimeout,
    generateIntermediateKey: Boolean,
    val loggerFactory: NamedLoggerFactory,
) {

  // This utility creates a new temporary topology manager.  It is mainly useful
  // to avoid passing around all the dependencies manually: we can just pass the
  // GenerateOnboardingTransactions object around instead.
  def createTemporaryTopologyManager(
      store: TopologyStore[TopologyStoreId.TemporaryStore]
  )(implicit
      ec: ExecutionContext
  ): TemporaryTopologyManager =
    new TemporaryTopologyManager(
      nodeId = nodeId,
      clock = clock,
      crypto = crypto,
      topologyCacheAggregatorConfig = batchingConfig.topologyCacheAggregator,
      topologyConfig = topologyConfig,
      store = store,
      timeouts = processingTimeout,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  def generate(
      topologyStore: TopologyStore[TopologyStoreId],
      topologyManager: TopologyManager[TopologyStoreId, BaseCrypto],
      protocolVersion: ProtocolVersion,
  )(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {

    implicit val traceContext: TraceContext = loggingContext.traceContext

    for {

      lookupKeyAndNeedRootCert <- determineTopologySigningKeyAndNeedForRootCertificate(
        store = topologyStore
      )
      (needRootCert, rootTopologySigningKey) = lookupKeyAndNeedRootCert

      _ <-
        if (needRootCert) {
          for {
            nsd <- EitherT
              .fromEither[FutureUnlessShutdown](
                NamespaceDelegation.create(
                  Namespace(rootTopologySigningKey.fingerprint),
                  rootTopologySigningKey,
                  DelegationRestriction.CanSignAllMappings,
                )
              )
              .leftMap(TopologyManagerError.InvalidTopologyMapping.Reject(_))

            _ <- topologyManager
              .proposeAndAuthorize(
                op = TopologyChangeOp.Replace,
                mapping = nsd,
                serial = None,
                signingKeys = Seq(rootTopologySigningKey.fingerprint),
                namespacesToSignFor = Seq.empty,
                protocolVersion = protocolVersion,
                expectFullAuthorization = true,
                waitToBecomeEffective = None,
              )
          } yield ()
        } else EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](())

      // create intermediate certificate if desired
      topologySigningKey <-
        if (
          generateIntermediateKey && rootTopologySigningKey.fingerprint == nodeId.namespace.fingerprint
        ) {
          loggingContext.info("Creating intermediate certificate for node")
          for {
            intermediateKey <- GenerateOnboardingTransactions.KeyHelper
              .getOrCreateSigningKey(crypto)(
                s"$instanceName-intermediate-${SigningKeyUsage.Namespace}",
                SigningKeyUsage.NamespaceOnly,
              )

            nsd <- EitherT
              .fromEither[FutureUnlessShutdown](
                NamespaceDelegation.create(
                  Namespace(rootTopologySigningKey.fingerprint),
                  intermediateKey,
                  DelegationRestriction.CanSignAllButNamespaceDelegations,
                )
              )
              .leftMap(TopologyManagerError.InvalidTopologyMapping.Reject(_))

            _ <- topologyManager
              .proposeAndAuthorize(
                op = TopologyChangeOp.Replace,
                mapping = nsd,
                serial = None,
                signingKeys = Seq(rootTopologySigningKey.fingerprint),
                namespacesToSignFor = Seq.empty,
                protocolVersion = protocolVersion,
                expectFullAuthorization = true,
                waitToBecomeEffective = None,
              )

          } yield intermediateKey
        } else
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](
            rootTopologySigningKey
          )

      // all nodes need two signing keys: (1) for sequencer authentication and (2) for protocol signing
      sequencerAuthKey <- GenerateOnboardingTransactions.KeyHelper
        .getOrCreateSigningKey(crypto)(
          s"$instanceName-${SigningKeyUsage.SequencerAuthentication.identifier}",
          SigningKeyUsage.SequencerAuthenticationOnly,
        )
      signingKey <- GenerateOnboardingTransactions.KeyHelper
        .getOrCreateSigningKey(crypto)(
          s"$instanceName-${SigningKeyUsage.Protocol.identifier}",
          SigningKeyUsage.ProtocolOnly,
        )

      // participants need also an encryption key
      keys <-
        if (member.code == ParticipantId.Code) {
          for {
            encryptionKey <- GenerateOnboardingTransactions.KeyHelper
              .getOrCreateEncryptionKey(crypto)(s"$instanceName-encryption")
          } yield NonEmpty.mk(Seq, sequencerAuthKey, signingKey, encryptionKey)
        } else {
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](
            NonEmpty.mk(Seq, sequencerAuthKey, signingKey)
          )
        }

      otk <- EitherT
        .fromEither[FutureUnlessShutdown](OwnerToKeyMapping.create(member, keys))
        .leftMap(TopologyManagerError.InvalidTopologyMapping.Reject(_))

      _ <- topologyManager
        .proposeAndAuthorize(
          op = TopologyChangeOp.Replace,
          mapping = otk,
          serial = None,
          signingKeys = Seq(
            topologySigningKey.fingerprint,
            sequencerAuthKey.fingerprint,
            signingKey.fingerprint,
          ),
          namespacesToSignFor = Seq.empty,
          protocolVersion = protocolVersion,
          expectFullAuthorization = true,
          waitToBecomeEffective = None,
        )

    } yield ()
  }

  /** Figure out the key we should be using to sign topology transactions
    *
    * We either use a delegated key (to which we have access) if we have certificates in our store.
    * Otherwise, we use the root key.
    *
    * If we have no certificates, we need to create a new root certificate. This is signalled using
    * the Boolean flag in the return value.
    */
  private def determineTopologySigningKeyAndNeedForRootCertificate(
      store: TopologyStore[TopologyStoreId]
  )(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    TopologyManagerError,
    (Boolean, SigningPublicKey),
  ] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    EitherT
      .right(
        store
          .findPositiveTransactions(
            CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(NamespaceDelegation.code),
            filterUid = None,
            filterNamespace = Some(NonEmpty(Seq, nodeId.namespace)),
          )
      )
      .flatMap { existing =>
        val possible = existing.collectOfMapping[NamespaceDelegation].result.map(_.mapping.target)
        if (possible.isEmpty) {
          crypto.cryptoPublicStore
            .signingKey(nodeId.fingerprint)
            .toRight[TopologyManagerError](
              TopologyManagerError.SecretKeyNotInStore.Failure(nodeId.fingerprint)
            )
            .map(key => (true, key))
        } else {
          // reverse so we find the lowest permissible one
          possible.reverse
            .findM(key => crypto.cryptoPrivateStore.existsSigningKey(key.fingerprint))
            .leftMap(
              TopologyManagerError.KeyStoreLookupError.Failure(_)
            )
            .subflatMap {
              case None =>
                Left(
                  TopologyManagerError.SecretKeyNotInStore
                    .Failures(possible.map(_.fingerprint).toSet)
                )
              case Some(key) => Right((false, key))
            }
        }
      }
  }
}

object GenerateOnboardingTransactions {
  object KeyHelper {

    def getOrCreateSigningKey(crypto: Crypto)(
        name: String,
        usage: NonEmpty[Set[SigningKeyUsage]],
    )(implicit
        loggingContext: ErrorLoggingContext,
        ec: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, SigningPublicKey] = {
      implicit val traceContext: TraceContext = loggingContext.traceContext
      getOrCreateKey(
        crypto.cryptoPublicStore.findSigningKeyIdByName,
        name =>
          crypto
            .generateSigningKey(usage = usage, name = name)
            .leftMap(err => TopologyManagerError.KeyGenerationError.Signing(err)),
        crypto.cryptoPrivateStore.existsSigningKey,
        name,
      )
    }

    def getOrCreateEncryptionKey(crypto: Crypto)(
        name: String
    )(implicit
        loggingContext: ErrorLoggingContext,
        ec: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, EncryptionPublicKey] = {
      implicit val traceContext: TraceContext = loggingContext.traceContext
      getOrCreateKey(
        crypto.cryptoPublicStore.findEncryptionKeyIdByName,
        name =>
          crypto
            .generateEncryptionKey(name = name)
            .leftMap(err => TopologyManagerError.KeyGenerationError.Encryption(err)),
        crypto.cryptoPrivateStore.existsDecryptionKey,
        name,
      )
    }

    private def getOrCreateKey[P <: PublicKey](
        findPubKeyIdByName: KeyName => OptionT[FutureUnlessShutdown, P],
        generateKey: Option[KeyName] => EitherT[
          FutureUnlessShutdown,
          TopologyManagerError,
          P,
        ],
        existPrivateKeyByFp: Fingerprint => EitherT[
          FutureUnlessShutdown,
          CryptoPrivateStoreError,
          Boolean,
        ],
        name: String,
    )(implicit
        loggingContext: ErrorLoggingContext,
        ec: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, P] = for {
      keyName <- EitherT
        .fromEither[FutureUnlessShutdown](KeyName.create(name))
        .leftMap(TopologyManagerError.KeyGenerationError.InvalidName(_))
      keyIdO <- EitherT.right(findPubKeyIdByName(keyName).value)
      pubKey <- keyIdO.fold(generateKey(Some(keyName))) { keyWithName =>
        val fingerprint = keyWithName.fingerprint
        existPrivateKeyByFp(fingerprint)
          .leftMap[TopologyManagerError](
            TopologyManagerError.KeyStoreLookupError.Failure(_)
          )
          .transform {
            case Right(true) => Right(keyWithName)
            case Right(false) =>
              Left(TopologyManagerError.SecretKeyNotInStore.Failures(Set(fingerprint)))
            case Left(err) => Left(err)
          }
      }
    } yield pubKey

  }

}
