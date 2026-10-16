// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scripts.offlinerootkey

import better.files.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyScriptsIntegrationTest.{
  offlineRootKeyPath,
  preparePath,
}
import com.digitalasset.canton.topology.transaction.TopologyTransaction
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.security.KeyPairGenerator
import scala.sys.process.{Process, ProcessLogger}

/** The store-based outbox recognizes a certificate that the operator provided for the
  * synchronizer's protocol version only if its hash is the hash of Canton's own serialization of
  * the converted transaction. The offline-root-key scripts must therefore serialize exactly as
  * Canton does, in the topology transaction version of the given protocol version.
  */
final class OfflineRootKeyScriptsSerializationTest
    extends AnyWordSpec
    with BaseTest
    with BeforeAndAfterAll {

  private lazy val dir: File = File.newTemporaryDirectory()

  override protected def afterAll(): Unit = {
    dir.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  private def publicKeyFile(name: String): String = {
    val file = dir / s"$name.der"
    file.writeByteArray(
      KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic.getEncoded
    )
    file.pathAsString
  }

  private lazy val rootKey = publicKeyFile("root")
  private lazy val targetKey = publicKeyFile("target")

  private def prepare(output: String, args: Seq[String]): File = {
    Process(
      preparePath.pathAsString +: (args ++ Seq("--output", (dir / output).pathAsString)),
      cwd = offlineRootKeyPath.toJava,
    ).!(ProcessLogger(logger.debug(_), logger.info(_))) shouldBe 0
    dir / s"$output.prep"
  }

  private def assertSerializedAsCanton(
      prepared: File,
      protocolVersion: ProtocolVersion,
  ): Unit = {
    val tx = TopologyTransaction
      .fromTrustedByteString(testedTopologyTransactionContext)(
        ByteString.copyFrom(prepared.byteArray)
      )
      .valueOrFail("parse prepared transaction")
    tx.representativeProtocolVersion shouldBe TopologyTransaction.protocolVersionRepresentativeFor(
      protocolVersion
    )
    val cantonTx = TopologyTransaction
      .create(tx.operation, tx.serial, tx.mapping, protocolVersion)
      .valueOrFail("create transaction")
    withClue(s"mapping ${tx.mapping}:") {
      tx.hash shouldBe cantonTx.hash
    }
  }

  ProtocolVersion.supported.forgetNE.foreach { protocolVersion =>
    s"prepare-cert.sh for protocol version $protocolVersion" should {
      def prepareFor(output: String, args: String*): File =
        prepare(
          s"$output-$protocolVersion",
          args ++ Seq("--protocol-version", protocolVersion.toString),
        )

      def restrictedDelegation(output: String, restrictions: String): File =
        prepareFor(
          output,
          "--delegation-restrictions",
          restrictions,
          "--root-pub-key",
          rootKey,
          "--target-pub-key",
          targetKey,
        )

      "serialize a root delegation as Canton does" in {
        assertSerializedAsCanton(
          prepareFor(
            "root",
            "--root-delegation",
            "--root-pub-key",
            rootKey,
            "--target-pub-key",
            rootKey,
          ),
          protocolVersion,
        )
      }

      "serialize an intermediate delegation and its revocation as Canton does" in {
        val delegation = prepareFor(
          "intermediate",
          "--intermediate-delegation",
          "--root-pub-key",
          rootKey,
          "--target-pub-key",
          targetKey,
        )
        assertSerializedAsCanton(delegation, protocolVersion)
        assertSerializedAsCanton(
          prepareFor("revocation", "--revoke-delegation", delegation.pathAsString),
          protocolVersion,
        )
      }

      "serialize delegation restrictions given in ascending order as Canton does" in {
        assertSerializedAsCanton(
          restrictedDelegation("restricted-sorted", "PARTY_TO_PARTICIPANT,PARTY_TO_KEY_MAPPING"),
          protocolVersion,
        )
      }

      "serialize delegation restrictions given in any order as Canton does" in {
        assertSerializedAsCanton(
          restrictedDelegation("restricted-unsorted", "PARTY_TO_KEY_MAPPING,PARTY_TO_PARTICIPANT"),
          protocolVersion,
        )
      }

      "serialize duplicate delegation restrictions as Canton does" in {
        assertSerializedAsCanton(
          restrictedDelegation(
            "restricted-duplicates",
            "PARTY_TO_KEY_MAPPING,PARTY_TO_PARTICIPANT,PARTY_TO_KEY_MAPPING",
          ),
          protocolVersion,
        )
      }
    }
  }
}
