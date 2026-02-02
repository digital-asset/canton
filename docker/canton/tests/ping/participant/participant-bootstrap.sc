import com.daml.nonempty.NonEmpty

import com.digitalasset.canton.console.LocalInstanceReference

def main() = {
  def participantInit(node: LocalInstanceReference): Unit = {
    // Create a signing key used to define the node identity.
    val namespaceKey =
      node.keys.secret
        .generate_signing_key(
          name = node.name + s"-${SigningKeyUsage.Namespace.identifier}",
          SigningKeyUsage.NamespaceOnly,
        )

    // Create a signing key used to authenticate the node toward the Sequencer.
    val sequencerAuthKey =
      node.keys.secret.generate_signing_key(
        name = node.name + s"-${SigningKeyUsage.SequencerAuthentication.identifier}",
        SigningKeyUsage.SequencerAuthenticationOnly,
      )

    // Create a signing key used to sign protocol messages.
    val signingKey =
      node.keys.secret
        .generate_signing_key(
          name = node.name + s"-${SigningKeyUsage.Protocol.identifier}",
          SigningKeyUsage.ProtocolOnly,
        )

    // Create the encryption key.
    val encryptionKey =
      node.keys.secret.generate_encryption_key(name = node.name + "-encryption")
    val namespace = Namespace(namespaceKey.id)
    node.topology.init_id_from_uid(
      UniqueIdentifier.tryCreate("manual-" + node.name, namespace)
    )

    // Wait until the node is ready to receive the node identity.
    node.health.wait_for_ready_for_node_topology()

    // Create the self-signed root certificate.
    node.topology.namespace_delegations.propose_delegation(
      namespace,
      namespaceKey,
      CanSignAllMappings,
    )

    // Assign the new keys to this node.
    node.topology.owner_to_key_mappings.propose(
      member = node.id.member,
      keys = NonEmpty(Seq, sequencerAuthKey, signingKey, encryptionKey),
      signedBy = Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
    )
    node.health.wait_for_initialized()
  }
  logger.info("=== init participant ===")

  participantInit(participant)
  logger.info("=== participant initalized ===")
  logger.info("=== connecting to synchronizer ===")

  participant.synchronizers.connect("da", s"http://sequencer:5008")
  logger.info("=== finished connecting to synchronizer ===")

  participant.health.ping(participant)
  logger.info("=== finishing participant bootstrap ===")

}
