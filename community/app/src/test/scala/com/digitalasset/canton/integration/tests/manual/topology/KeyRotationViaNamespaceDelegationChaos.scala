// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Mutex

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}

private[topology] class KeyRotationViaNamespaceDelegationChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "KeyRotationChaos"

  override def companion: TopologyOperationsCompanion = KeyRotationChaos

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*
    participants.local
      .parTraverse_(participant => Future(keyRotation(participant)))
  }

  def keyRotation(node: ParticipantReference)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Unit = {
    import env.*
    whenNodeReadyAndActive(node, KeyRotationChaos.idleTime) {
      withOperation_("keyRotationViaNamespaceDelegation")(s"rotating key for node ${node.name}") {
        // create a new namespace intermediate key
        val intermediateKey =
          node.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

        // Create a namespace delegation for the intermediate key with the namespace root key
        node.topology.namespace_delegations.propose_delegation(
          node.namespace,
          intermediateKey,
          CanSignAllButNamespaceDelegations,
          store = daId,
        )

        val newKey =
          node.keys.secret.generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)

        KeyRotationChaos.exclusively(node) {
          val currentKey = getCurrentKey(
            node = node,
            purpose = KeyPurpose.Signing,
            usage = SigningKeyUsage.ProtocolOnly,
            synchronizerId = daId,
          )

          node.topology.owner_to_key_mappings.add_key(
            key = newKey.fingerprint,
            purpose = KeyPurpose.Signing,
            signedBy = Seq(intermediateKey.fingerprint, newKey.fingerprint),
          )

          node.topology.owner_to_key_mappings.remove_key(
            key = currentKey.fingerprint,
            purpose = KeyPurpose.Signing,
            signedBy = Seq(intermediateKey.fingerprint),
          )

        }

        // Remove the previous intermediate key
        node.topology.namespace_delegations.propose_revocation(
          node.namespace,
          intermediateKey,
          store = daId,
        )
      }
      node.health.ping(node.id, timeout = 60.seconds)
    }
  }
}

object KeyRotationChaos extends TopologyOperationsCompanion {

  val idleTime: FiniteDuration = 25.seconds

  private val rotationLocks = TrieMap.empty[String, Mutex]

  /** Runs `rotation` while no other key rotation runs on `node`.
    *
    * Key rotations on the same node by different chaos operations would otherwise race: both read
    * the owner-to-key mapping from the synchronizer store and propose the next serial so one of
    * them fails.
    */
  def exclusively[A](node: InstanceReference)(rotation: => A): A =
    blocking(rotationLocks.getOrElseUpdate(node.name, Mutex()).exclusive(rotation))

  override def acceptableLogEntries: Seq[String] = Seq(
    "periodic acknowledgement failed",
    "Request failed for sequencer",
    // Other operations on a node can sign topology transactions with the intermediate namespace
    // key, which can be revoked before those transactions are validated.
    "No delegation found for keys",
    // A concurrent rotation on the same node already proposed the same owner-to-key mapping
    "ALREADY_EXISTS/TOPOLOGY_MAPPING_ALREADY_EXISTS",
  )

  override def acceptableNonRetryableLogEntries: Seq[String] = Seq(
    // Wrong key
    "used to generate signature is not a valid key for"
  )
}
