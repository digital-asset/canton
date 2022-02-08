// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.instances.future._
import cats.syntax.either._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.topology.transaction.{
  LegalIdentityClaim,
  LegalIdentityClaimEvidence,
  TopologyStateUpdate,
  SignedLegalIdentityClaim,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class LegalIdentityInit(certificateGenerator: X509CertificateGenerator, crypto: Crypto)(implicit
    ec: ExecutionContext,
    traceContext: TraceContext,
) {
  def generateCertificate[E](
      uid: UniqueIdentifier,
      alternativeNames: Seq[Member],
  ): EitherT[Future, String, X509Certificate] =
    generateCertificate(uid.toProtoPrimitive, alternativeNames.map(_.toProtoPrimitive))

  def generateCertificate[E](
      commonName: String,
      subjectAlternativeNames: Seq[String],
  ): EitherT[Future, String, X509Certificate] =
    for {
      // Generate a signing key for a certificate, uses EC-DSA with NIST P384 for CCF compatibility and "raw" encoding for serialization to/from certificates.
      certKey <- crypto
        .generateSigningKey(SigningKeyScheme.EcDsaP384)
        .leftMap(err => s"Failed to generate signing key pair for certificate: $err")
      // generate and store certificate
      cert <- certificateGenerator
        .generate(commonName, certKey.id, subjectAlternativeNames)
        .leftMap(err => s"Failed to generate X509 certificate for legal identity: $err")
      _ <- crypto.cryptoPublicStore
        .storeCertificate(cert)
        .leftMap(err => s"Failed to store generated certificate: $err")
    } yield cert

  def initializeCertificate[E <: CantonError](
      uid: UniqueIdentifier,
      alternativeNames: Seq[Member],
      namespaceKey: PublicKey,
  )(topologyManager: TopologyManager[E]): EitherT[Future, String, Unit] =
    for {
      cert <- generateCertificate(uid, alternativeNames)

      evidence <- cert.toPem
        .map(LegalIdentityClaimEvidence.X509Cert)
        .leftMap(err => s"Failed to serialize certificate to PEM: $err")
        .toEitherT
      claim = LegalIdentityClaim.create(uid, evidence)
      claimHash = claim.hash(crypto.pureCrypto)

      // Sign the legal identity claim with the legal entity key as specified in the evidence
      certKey <- cert
        .publicKey(crypto.javaKeyConverter)
        .leftMap(err => s"Failed to extract public key from certificate: $err")
        .toEitherT
      claimSig <- crypto.privateCrypto
        .sign(claimHash, certKey.fingerprint)
        .leftMap(err => s"Failed to sign legal identity claim: $err")

      // Authorize the legal identity mapping with the namespace key
      _ <- topologyManager
        .authorize(
          TopologyStateUpdate.createAdd(
            SignedLegalIdentityClaim(uid, claim.getCryptographicEvidence, claimSig)
          ),
          Some(namespaceKey.fingerprint),
          false,
        )
        .leftMap(_.toString)
    } yield ()
}
