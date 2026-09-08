{
  import com.digitalasset.nonempty.NonEmpty

  import com.digitalasset.canton.console.{LocalInstanceReference, ParticipantReference}

  def nodeInit(node: LocalInstanceReference) = {

    node.health.wait_for_ready_for_id()

    // Generate the root key.
    val namespaceKey =
      node.keys.secret
        .generate_signing_key(
          name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
          usage = SigningKeyUsage.NamespaceOnly,
        )

    // initialize the node id
    val namespace = Namespace(namespaceKey.id)
    node.topology.init_id_from_uid(
      UniqueIdentifier.tryCreate("manual-" + node.name, namespace)
    )

    node match {
      case _: ParticipantReference =>
        // Wait until the node is ready to receive the node topology.
        node.health.wait_for_ready_for_node_topology()

        val encryptionKey =
          node.keys.secret.generate_encryption_key(name = node.name + "-encryption")

        val signingKey =
          node.keys.secret
            .generate_signing_key(
              name = s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
              usage = SigningKeyUsage.ProtocolOnly,
            )

        // create a sequencer authentication signing key for the mediator
        val sequencerAuthKey = node.keys.secret
          .generate_signing_key(
            s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
            usage = SigningKeyUsage.SequencerAuthenticationOnly,
          )

        // Create the self-signed root certificate.
        val nsd = node.topology.namespace_delegations.propose_delegation(
          namespace,
          namespaceKey,
          CanSignAllMappings,
        )

        // Assign the new keys to this node.
        val otk = node.topology.owner_to_key_mappings.propose(
          member = node.id.member,
          keys = NonEmpty(Seq, sequencerAuthKey, signingKey, encryptionKey),
          signedBy = Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
        )
        node.health.wait_for_initialized()
      case _: LocalInstanceReference => //

        // Sequencer & mediator will generate the topology on the fly, so we are done.
        node.health.wait_for_ready_for_initialization()
    }
  }

  nodes.local.foreach(nodeInit)
  bootstrap.synchronizer(
    "synchronizer",
    synchronizerOwners = Seq(sequencer),
    sequencers = Seq(sequencer),
    mediators = Seq(mediator),
    synchronizerThreshold = PositiveInt.one,
    staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer),
  )
  participant.synchronizers.connect_local(sequencer, alias = "synchronizer")
  utils.retry_until_true {
    participant.synchronizers.active("synchronizer")
  }
  participant.health.ping(participant)
}
