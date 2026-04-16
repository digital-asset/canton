// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.crypto.{EncryptionPublicKey, Fingerprint}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.bouncycastle.crypto.AsymmetricCipherKeyPair
import org.bouncycastle.crypto.agreement.ECDHBasicAgreement
import org.bouncycastle.crypto.generators.ECKeyPairGenerator
import org.bouncycastle.crypto.params.*
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey
import org.bouncycastle.jce.ECNamedCurveTable

import java.math.BigInteger
import java.security.{PublicKey as JPublicKey, SecureRandom}
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Precomputes ECDH shared secrets with known participants in the background.
  * Each entry is consumed exactly once (forward secrecy) and automatically refilled.
  *
  * This moves the expensive ECDH key agreement (~0.36ms per recipient) off the
  * transaction critical path. When encryptFor is called, the shared secret is
  * already computed — only KDF + AES remains (~0.03ms).
  */
class EcdhPrecomputeCache(
    override val loggerFactory: NamedLoggerFactory
) extends NamedLogging
    with AutoCloseable {

  private val ecSpec = ECNamedCurveTable.getParameterSpec("secp256r1")
  private val ecDomain =
    new ECDomainParameters(ecSpec.getCurve, ecSpec.getG, ecSpec.getN, ecSpec.getH)

  /** Precomputed ECDH result: ephemeral keypair + shared secret for a specific recipient */
  case class PrecomputedEcdh(
      ephemeralKeyPair: AsymmetricCipherKeyPair,
      sharedSecret: BigInteger,
  )

  // One precomputed entry per recipient public key fingerprint.
  // ConcurrentHashMap for thread-safe consume-and-refill.
  private val cache = new ConcurrentHashMap[Fingerprint, PrecomputedEcdh]()

  // Known recipient public keys for background refill
  private val knownRecipients = new ConcurrentHashMap[Fingerprint, ECPublicKeyParameters]()

  private val executor = Executors.newSingleThreadScheduledExecutor(r => {
    val t = new Thread(r, "ecdh-precompute")
    t.setDaemon(true)
    t
  })

  /** Register recipient public keys for background precomputation.
    * Call this on topology changes or participant connection.
    */
  def registerRecipients(
      publicKeys: Seq[EncryptionPublicKey],
      javaKeyLookup: EncryptionPublicKey => Option[JPublicKey],
  )(implicit traceContext: TraceContext): Unit = {
    var newCount = 0
    publicKeys.foreach { pk =>
      if (!knownRecipients.containsKey(pk.fingerprint)) {
        javaKeyLookup(pk).foreach {
          case bcKey: BCECPublicKey =>
            val params = new ECPublicKeyParameters(bcKey.engineGetQ(), ecDomain)
            knownRecipients.put(pk.fingerprint, params)
            newCount += 1
          case jcaKey: java.security.interfaces.ECPublicKey =>
            val point =
              ecSpec.getCurve.createPoint(jcaKey.getW.getAffineX, jcaKey.getW.getAffineY)
            val params = new ECPublicKeyParameters(point, ecDomain)
            knownRecipients.put(pk.fingerprint, params)
            newCount += 1
          case _ => // unsupported key type, skip
        }
      }
    }
    if (newCount > 0) {
      logger.debug(s"Registered $newCount new recipients for ECDH precomputation")
      // Immediately precompute for new recipients
      executor.submit(new Runnable { def run(): Unit = refillCache()(traceContext) })
    }
  }

  /** Consume a precomputed ECDH result for the given recipient.
    * Returns None if no precomputed result is available (caller should fall back to full ECIES).
    * The entry is removed from the cache (one-use for forward secrecy) and a background refill
    * is triggered.
    */
  def consume(fingerprint: Fingerprint)(implicit traceContext: TraceContext): Option[PrecomputedEcdh] = {
    val result = Option(cache.remove(fingerprint))
    if (result.isDefined) {
      // Schedule background refill for this fingerprint
      executor.submit(new Runnable {
        def run(): Unit = precomputeFor(fingerprint)(traceContext)
      })
    }
    result
  }

  /** Precompute ECDH for all known recipients that are missing from the cache. */
  private def refillCache()(implicit traceContext: TraceContext): Unit =
    knownRecipients.asScala.foreach { case (fingerprint, _) =>
      if (!cache.containsKey(fingerprint)) {
        precomputeFor(fingerprint)
      }
    }

  /** Precompute ECDH for a single recipient. */
  private def precomputeFor(fingerprint: Fingerprint)(implicit traceContext: TraceContext): Unit =
    Option(knownRecipients.get(fingerprint)).foreach { recipientParams =>
      try {
        val random = JceSecureRandom.random.get()
        val kpg = new ECKeyPairGenerator()
        kpg.init(new ECKeyGenerationParameters(ecDomain, random))
        val ephemeral = kpg.generateKeyPair()

        val agree = new ECDHBasicAgreement()
        agree.init(ephemeral.getPrivate)
        val sharedSecret = agree.calculateAgreement(recipientParams)

        cache.put(fingerprint, PrecomputedEcdh(ephemeral, sharedSecret))
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to precompute ECDH for $fingerprint: ${e.getMessage}")
      }
    }

  /** Start periodic background refill. */
  def startPeriodicRefill(intervalMs: Long = 100): Unit =
    executor.scheduleAtFixedRate(
      () => refillCache()(TraceContext.empty),
      0,
      intervalMs,
      TimeUnit.MILLISECONDS,
    )

  /** Number of precomputed entries currently available. */
  def size: Int = cache.size()

  override def close(): Unit = {
    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.SECONDS)
  }
}
