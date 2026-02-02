{
  import com.daml.nonempty.NonEmpty

  import com.digitalasset.canton.console.{LocalInstanceReference, ParticipantReference}

  def nodeInit(node: LocalInstanceReference): Unit = {

    val signingKey =
      node.keys.secret
        .generate_signing_key(
          name = s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
          usage = SigningKeyUsage.ProtocolOnly,
        )

    val namespaceKey =
      node.keys.secret
        .generate_signing_key(
          name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
          usage = SigningKeyUsage.NamespaceOnly,
        )

    // create a sequencer authentication signing key for the mediator
    val sequencerAuthKey = node.keys.secret
      .generate_signing_key(
        s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
        usage = SigningKeyUsage.SequencerAuthenticationOnly,
      )
    node.health.wait_for_ready_for_id()

    node match {
      case _: ParticipantReference =>
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
      case n: LocalInstanceReference => //

        // initialize the node id
        n.topology.init_id_from_uid(
          UniqueIdentifier.tryCreate(node.name, namespaceKey.fingerprint)
        )

        n.health.wait_for_ready_for_node_topology()

        n.topology.namespace_delegations.propose_delegation(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          CanSignAllMappings,
        )


        val keys = NonEmpty(Seq, signingKey, sequencerAuthKey)

        n.topology.owner_to_key_mappings.propose(
          member = node.id.member,
          keys = keys,
          signedBy = (namespaceKey +: keys).map(_.fingerprint),
        )

        n.health.wait_for_ready_for_initialization()
    }
  }




  nodes.local.foreach(nodeInit)
  bootstrap.synchronizer("synchronizer", synchronizerOwners = Seq(sequencer), sequencers = Seq(sequencer), mediators = Seq(mediator), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
  participant.synchronizers.connect_local(sequencer, alias = "synchronizer")
  utils.retry_until_true {
    participant.synchronizers.active("synchronizer")
  }
  participant.health.ping(participant)
}
