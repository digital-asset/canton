// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.scripts.offlinerootkey

import better.files.*
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.{SigningKeySpec, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyCertificates.OfflineRoot
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyScriptsIntegrationTest.{
  assemblePath,
  offlineRootKeyPath,
  preparePath,
}
import com.digitalasset.canton.integration.{CommunityIntegrationTest, TestConsoleEnvironment}
import com.digitalasset.canton.topology.transaction.TopologyTransaction
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterAll

import scala.collection.concurrent.TrieMap
import scala.sys.process.Process

/** Namespace certificates of participants whose root namespace key is offline, made with the
  * offline-root-key scripts and signed with a root key held in `global_secret`.
  */
trait OfflineRootKeyCertificates extends BeforeAndAfterAll { self: CommunityIntegrationTest =>

  protected val offlineRoots: TrieMap[String, OfflineRoot] = TrieMap.empty[String, OfflineRoot]

  protected lazy val certificatesDir: File = File.newTemporaryDirectory()

  abstract override def afterAll(): Unit = {
    certificatesDir.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  protected def protoVersion(protocolVersion: ProtocolVersion): String =
    TopologyTransaction
      .protoVersionFor(protocolVersion)
      .value
      .v
      .toString

  private def runScript(script: File, args: Seq[String]): Unit =
    Process(script.pathAsString +: args, cwd = offlineRootKeyPath.toJava)
      .!(mkProcessLogger()) shouldBe 0

  protected def certificatePrefix(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  ): String =
    (certificatesDir / s"${participant.name}-${protoVersion(protocolVersion)}").pathAsString

  /** Prepares a topology transaction for the given protocol version with the offline-root-key
    * scripts, signs it with the offline root key, and assembles the certificate. Returns the path
    * of the certificate.
    */
  protected def offlineCertificate(
      rootKey: SigningPublicKey,
      output: String,
      protocolVersion: ProtocolVersion,
      prepareArgs: Seq[String],
  )(implicit env: TestConsoleEnvironment): String = {
    import env.*
    runScript(
      preparePath,
      prepareArgs ++ Seq(
        "--protocol-version",
        protocolVersion.toString,
        "--output",
        output,
      ),
    )
    val signature = File(s"$output.sig")
    signature.writeByteArray(
      global_secret
        .sign(
          ByteString.copyFrom(File(s"$output.hash").byteArray),
          rootKey.fingerprint,
          SigningKeyUsage.NamespaceOnly,
        )
        .toProtoV30
        .signature
        .toByteArray
    )
    runScript(
      assemblePath,
      Seq(
        "--prepared-transaction",
        s"$output.prep",
        "--signature",
        signature.pathAsString,
        "--signature-algorithm",
        "ed25519",
        "--output",
        output,
      ),
    )
    s"$output.cert"
  }

  /** The root and intermediate namespace certificates, signed with the offline root key for the
    * given protocol version (what the operator does with the offline root key tooling).
    */
  protected def namespaceCertificates(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  )(implicit env: TestConsoleEnvironment): Seq[String] = {
    val root = offlineRoots(participant.name)
    val prefix = certificatePrefix(participant, protocolVersion)
    val intermediateKeyFile = s"$prefix-intermediate-key.pub"
    participant.keys.public.download_to(root.intermediateKey.id, intermediateKeyFile)
    Seq(
      offlineCertificate(
        root.rootKey,
        s"$prefix-root",
        protocolVersion,
        Seq(
          "--root-delegation",
          "--root-pub-key",
          root.rootKeyFile,
          "--target-pub-key",
          root.rootKeyFile,
        ),
      ),
      offlineCertificate(
        root.rootKey,
        s"$prefix-intermediate",
        protocolVersion,
        Seq(
          "--intermediate-delegation",
          "--root-pub-key",
          root.rootKeyFile,
          "--canton-target-pub-key",
          intermediateKeyFile,
        ),
      ),
    )
  }

  /** Initializes the participant with an offline root namespace key held in `global_secret` and an
    * intermediate namespace key on the node, with certificates signed for the given protocol
    * version.
    */
  protected def initializeWithOfflineRoot(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participant.health.wait_for_ready_for_id()
    val intermediateKey = participant.keys.secret
      .generate_signing_key(
        s"${participant.name}-intermediate",
        SigningKeyUsage.NamespaceOnly,
      )
    val rootKey = global_secret.keys.secret
      .generate_key(SigningKeySpec.EcCurve25519, SigningKeyUsage.NamespaceOnly)
    val rootKeyFile = certificatesDir / s"${participant.name}-root-key.der"
    rootKeyFile.writeByteArray(
      rootKey.toProtoV30.valueOrFail("serializing public key").publicKey.toByteArray
    )
    offlineRoots.update(
      participant.name,
      OfflineRoot(rootKey, rootKeyFile.pathAsString, intermediateKey),
    )

    participant.topology.init_id_from_uid(
      UniqueIdentifier.tryCreate(participant.name, Namespace(rootKey.id)),
      delegationFiles = namespaceCertificates(participant, protocolVersion),
    )
    eventually()(participant.is_initialized shouldBe true)
    participant.keys.secret.list(filterFingerprint = rootKey.id.unwrap) shouldBe empty
  }
}

object OfflineRootKeyCertificates {
  final case class OfflineRoot(
      rootKey: SigningPublicKey,
      rootKeyFile: String,
      intermediateKey: SigningPublicKey,
  )
}
