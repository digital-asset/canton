// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.bouncycastle.asn1.x509.AlgorithmIdentifier

import java.security.{PrivateKey => JPrivateKey, PublicKey => JPublicKey}
import scala.concurrent.ExecutionContext

object SymbolicCrypto extends LazyLogging {

  private val keyData: ByteString = ByteString.copyFromUtf8("symbolic_crypto_key_data")

  // Note: The scheme is ignored for symbolic keys
  def signingPublicKey(keyId: String): SigningPublicKey = signingPublicKey(
    Fingerprint.tryCreate(keyId)
  )

  def signingPublicKey(keyId: Fingerprint): SigningPublicKey =
    new SigningPublicKey(keyId, CryptoKeyFormat.Symbolic, keyData, SigningKeyScheme.Ed25519)

  // Randomly generated private key with the given fingerprint
  def signingPrivateKey(keyId: Fingerprint): SigningPrivateKey =
    new SigningPrivateKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      SigningKeyScheme.Ed25519,
    )

  def signingPrivateKey(keyId: String): SigningPrivateKey = signingPrivateKey(
    Fingerprint.tryCreate(keyId)
  )

  def encryptionPublicKey(keyId: Fingerprint): EncryptionPublicKey =
    new EncryptionPublicKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
    )

  def encryptionPublicKey(keyId: String): EncryptionPublicKey = encryptionPublicKey(
    Fingerprint.tryCreate(keyId)
  )

  // Randomly generated private key with the given fingerprint
  def encryptionPrivateKey(keyId: Fingerprint): EncryptionPrivateKey =
    new EncryptionPrivateKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
    )

  def encryptionPrivateKey(keyId: String): EncryptionPrivateKey =
    encryptionPrivateKey(Fingerprint.tryCreate(keyId))

  def signature(signature: ByteString, signedBy: Fingerprint): Signature =
    SymbolicPureCrypto.createSignature(signature, signedBy, 0xffffffff)

  def emptySignature: Signature =
    signature(ByteString.EMPTY, Fingerprint.create(ByteString.EMPTY, HashAlgorithm.Sha256))

  def create(
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Crypto = {
    implicit val ec: ExecutionContext = DirectExecutionContext(TracedLogger(logger))

    val pureCrypto = new SymbolicPureCrypto()
    val cryptoPublicStore = new InMemoryCryptoPublicStore
    val cryptoPrivateStore = new InMemoryCryptoPrivateStore(loggerFactory)
    val privateCrypto = new SymbolicPrivateCrypto(pureCrypto, cryptoPrivateStore)

    // Conversion to java keys is not supported by symbolic crypto
    val javaKeyConverter = new JavaKeyConverter {
      override def toJava(privateKey: PrivateKey): Either[JavaKeyConversionError, JPrivateKey] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )

      override def toJava(
          publicKey: PublicKey
      ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )

      override def fromJavaSigningKey(
          publicKey: JPublicKey,
          algorithmIdentifier: AlgorithmIdentifier,
      ): Either[JavaKeyConversionError, SigningPublicKey] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )
    }

    new Crypto(
      pureCrypto,
      privateCrypto,
      cryptoPrivateStore,
      cryptoPublicStore,
      javaKeyConverter,
      timeouts,
      loggerFactory,
    )
  }

  /** Create symbolic crypto and pre-populate with keys using the given fingerprint suffixes, which will be prepended with the type of key (sigK, encK), and the fingerprints used for signing keys. */
  // TODO(#10059,Soren) Do not discard an EitherT
  @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
  def tryCreate(
      signingFingerprints: Seq[Fingerprint],
      fingerprintSuffixes: Seq[String],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Crypto = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty._

    val crypto = SymbolicCrypto.create(timeouts, loggerFactory)

    // Create a keypair for each signing fingerprint
    signingFingerprints.foreach { k =>
      val sigPrivKey = SymbolicCrypto.signingPrivateKey(k)
      val sigPubKey = SymbolicCrypto.signingPublicKey(k)

      crypto.cryptoPrivateStore.storeSigningKey(sigPrivKey, None)
      crypto.cryptoPublicStore.storeSigningKey(sigPubKey)
    }

    // For the fingerprint suffixes, create both encryption and signing keys with a `encK-` or `sigK-` prefix.
    fingerprintSuffixes.foreach { k =>
      val sigKeyId = s"sigK-$k"
      val sigPrivKey = SymbolicCrypto.signingPrivateKey(sigKeyId)
      val sigPubKey = SymbolicCrypto.signingPublicKey(sigKeyId)

      val encKeyId = s"encK-$k"
      val encPrivKey = SymbolicCrypto.encryptionPrivateKey(encKeyId)
      val encPubKey = SymbolicCrypto.encryptionPublicKey(encKeyId)

      crypto.cryptoPrivateStore.storeSigningKey(sigPrivKey, None)
      crypto.cryptoPublicStore.storeSigningKey(sigPubKey)
      crypto.cryptoPrivateStore.storeDecryptionKey(encPrivKey, None)
      crypto.cryptoPublicStore.storeEncryptionKey(encPubKey)
    }

    crypto
  }

}
