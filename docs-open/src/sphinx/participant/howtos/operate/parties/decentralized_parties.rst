..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _howto_decentralized_parties:

============================
Decentralized party overview
============================

A decentralized party combines three different features:

* Decentralization of topology management of the party: A `decentralized namespace` to ensure that any topology transaction
  for that party requires signatures from a threshold of keys.
* Decentralization of transaction confirmations for the party: A `party to participant mapping` containing multiple confirming
  participants and a threshold to ensure that transactions requiring
  confirmations from that party also require confirmation from a
  threshold of participant nodes.
* Decentralization of transaction submissions for the party: Optionally, a `party to key mapping` to support submitting
  transactions that require direct authorization of the external
  party, for example creating a contract that the party is a signatory on by
  signing the prepared transaction with a threshold of keys. If no party-to-key mapping
  is defined, then the initial contracts need to be created when the
  `party to participant` threshold is 1 (if this was ever the case), and a node has submission rights, not just
  confirmation rights.

Setup a decentralized party
===========================

While the decentralized namespace, the party to participant mapping,
and the party to key mapping can be configured fully independently, a
common scenario is that a set of entities jointly control all three
i.e. all three have the same number of members and the same
threshold. The instructions here describe that setup with the three members being `alice`, `bob`, and `charlie`, who use `participant1`, `participant2`, and `participant3` respectively.

First generate the keys used for the decentralized namespace:

.. snippet:: decentralized_parties
    .. success:: val aliceNamespaceKey = participant1.keys.secret.generate_signing_key("decentralized-party-namespace", SigningKeyUsage.NamespaceOnly)
    .. success:: val bobNamespaceKey = participant2.keys.secret.generate_signing_key("decentralized-party-namespace", SigningKeyUsage.NamespaceOnly)
    .. success:: val charlieNamespaceKey = participant3.keys.secret.generate_signing_key("decentralized-party-namespace", SigningKeyUsage.NamespaceOnly)
    .. success:: val aliceNamespace = Namespace(aliceNamespaceKey.fingerprint)
    .. success:: val bobNamespace = Namespace(bobNamespaceKey.fingerprint)
    .. success:: val charlieNamespace = Namespace(charlieNamespaceKey.fingerprint)
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "global",
          sequencers = Seq(sequencerBanking),
          mediators = Seq(mediatorBanking),
          synchronizerOwners = Seq(sequencerBanking),
          synchronizerThreshold = 1,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
    .. hidden:: participant1.synchronizers.connect_local(sequencerBanking, "global")
    .. hidden:: participant2.synchronizers.connect_local(sequencerBanking, "global")
    .. hidden:: participant3.synchronizers.connect_local(sequencerBanking, "global")

Next, each node publishes the namespace delegation for that key to the synchronizer. This makes the key known to all nodes connected to the synchronizer and allows it to be used in later transactions:

.. snippet:: decentralized_parties
     .. success:: val synchronizerId = participant1.synchronizers.id_of(com.digitalasset.canton.SynchronizerAlias.tryCreate("global"))
     .. success:: participant1.topology.namespace_delegations.propose_delegation(aliceNamespace, aliceNamespaceKey, DelegationRestriction.CanSignAllMappings, store = synchronizerId)
     .. success:: participant2.topology.namespace_delegations.propose_delegation(bobNamespace, bobNamespaceKey, DelegationRestriction.CanSignAllMappings, store = synchronizerId)
     .. success:: participant3.topology.namespace_delegations.propose_delegation(charlieNamespace, charlieNamespaceKey, DelegationRestriction.CanSignAllMappings, store = synchronizerId)

Once the namespace delegations are published, you can create the
decentralized namespace definition. For this, each node needs to sign
and publish the same topology transaction to the synchronizer. They
also need to choose a threshold, which determines how many
signatures from the owners of the decentralized namespace are required for
a topology transaction to be authorized on behalf of the decentralized namespace.
This example uses a threshold of two. Note that the threshold does
not apply to the initial transaction that establishes the
decentralized namespace. For that, signatures from all owners are
required, not just a threshold. Once all nodes publish their signed
transaction, the decentralized namespace transaction shows up in the ``list``
command:

.. snippet:: decentralized_parties
     .. success:: val namespaceDef = DecentralizedNamespaceDefinition.tryCreate(DecentralizedNamespaceDefinition.computeNamespace(Set(aliceNamespace, bobNamespace, charlieNamespace)), PositiveInt.tryCreate(2), com.daml.nonempty.NonEmpty(Set, aliceNamespace, bobNamespace, charlieNamespace))
     .. success:: participant1.topology.decentralized_namespaces.propose(namespaceDef, store = synchronizerId)
     .. success:: participant2.topology.decentralized_namespaces.propose(namespaceDef, store = synchronizerId)
     .. success:: participant3.topology.decentralized_namespaces.propose(namespaceDef, store = synchronizerId)
     .. success:: utils.retry_until_true(participant1.topology.decentralized_namespaces.list(synchronizerId, filterNamespace = namespaceDef.namespace.filterString).nonEmpty)

The next step is to set up the ``PartyToParticipant`` mapping. For
this, you need to chose a prefix for the party. The full party ID is
then ``prefix::namespace``. This example uses
``decentralized-party`` as the prefix. You also need to specify the
list of participants that should host that party, the permissions
(this should be ``Confirmation`` for all nodes participating in
consensus for that party, but you may have additional read-only nodes
with ``Observation`` permissions), and a threshold. The threshold
determines how many confirmations are required for the decentralized
party. This example uses the same threshold of two used
for the decentralized namespace. As for the decentralized
namespace, each node independently publishes the transaction; once
all of them publish their transactions it becomes valid and shows up in ``list``:

.. snippet:: decentralized_parties
     .. success:: val partyId = PartyId(UniqueIdentifier.tryCreate("decentralized-party", namespaceDef.namespace))
     .. success:: participant1.topology.party_to_participant_mappings.propose(partyId, Seq((participant1, ParticipantPermission.Confirmation), (participant2, ParticipantPermission.Confirmation), (participant3, ParticipantPermission.Confirmation)), PositiveInt.tryCreate(2), store = synchronizerId)
     .. success:: participant2.topology.party_to_participant_mappings.propose(partyId, Seq((participant1, ParticipantPermission.Confirmation), (participant2, ParticipantPermission.Confirmation), (participant3, ParticipantPermission.Confirmation)), PositiveInt.tryCreate(2), store = synchronizerId)
     .. success:: participant3.topology.party_to_participant_mappings.propose(partyId, Seq((participant1, ParticipantPermission.Confirmation), (participant2, ParticipantPermission.Confirmation), (participant3, ParticipantPermission.Confirmation)), PositiveInt.tryCreate(2), store = synchronizerId)
     .. success:: utils.retry_until_true(participant3.topology.party_to_participant_mappings.list(synchronizerId, filterParty = partyId.filterString).nonEmpty)

The last (optional) step is to set up the party to key mapping. This
allows submitting transactions directly as the decentralized party
through aggregating signatures offline. It is possible to reuse the
same keys here that are used for the decentralized namespace (provided
you change the SigningKeyUsage to be less restrictive) but we use
separate keys here:

.. snippet:: decentralized_parties
     .. success:: val aliceDamlKey = participant1.keys.secret.generate_signing_key("decentralized-party-daml-transactions", SigningKeyUsage.ProtocolOnly)
     .. success:: val bobDamlKey = participant2.keys.secret.generate_signing_key("decentralized-party-daml-transactions", SigningKeyUsage.ProtocolOnly)
     .. success:: val charlieDamlKey = participant3.keys.secret.generate_signing_key("decentralized-party-daml-transactions", SigningKeyUsage.ProtocolOnly)

With the keys set up, you can now create the ``PartyToKey`` topology transaction. Use a threshold of two signatures again:

.. snippet:: decentralized_parties
     .. success:: participant1.topology.party_to_key_mappings.propose(partyId, PositiveInt.tryCreate(2), com.daml.nonempty.NonEmpty(Seq, aliceDamlKey, bobDamlKey, charlieDamlKey), store = synchronizerId, mustFullyAuthorize = false)
     .. success:: participant2.topology.party_to_key_mappings.propose(partyId, PositiveInt.tryCreate(2), com.daml.nonempty.NonEmpty(Seq, aliceDamlKey, bobDamlKey, charlieDamlKey), store = synchronizerId, mustFullyAuthorize = false)
     .. success:: participant3.topology.party_to_key_mappings.propose(partyId, PositiveInt.tryCreate(2), com.daml.nonempty.NonEmpty(Seq, aliceDamlKey, bobDamlKey, charlieDamlKey), store = synchronizerId, mustFullyAuthorize = false)
     .. success:: utils.retry_until_true(participant3.topology.party_to_key_mappings.list(store = synchronizerId, filterParty = partyId.filterString).nonEmpty)

With that, the party is fully set up and can be used.

Changing the set of members
===========================

To add and remove members, the steps are the same: There is a threshold of the
existing members and any new members must submit the three topology
transactions. It is also possible to only add them to some, but not all,
of the three mappings, but usually it makes sense to keep the three in
sync.

Note that adding a member to ``PartyToParticipant`` requires not just
a topology transaction but a full party migration including an ACS
export and import. The details of this are outside of the scope of
this topic.

Next steps
==========

For details on how to submit an externally signed Daml transaction enabled by the ``PartyToKey`` mapping, refer to the :externalref:`external submission docs <tutorial_externally_signed_transactions_part_2>`.

In this tutorial, both the namespace and protocol keys are held by the participant itself. It is also possible to hold them outside of the participant. The actual flow stays the same, but each submission of a topology transaction must be signed externally. Refer to the :externalref:`external topology signing docs <tutorial_onboard_external_party>` for details on how to do this.

Decentralized namespace computation
===================================

In the above example, we used
``DecentralizedNamespaceDefinition.computeNamespace(Set(aliceNamespace, bobNamespace, charlieNamespace))`` to compute the decentralized
namespace from the namespaces of the initial owners. Note that only the initial owners matter here, the decentralized namespace does not change as owners get added or removed.

However, in some cases you might not run this from a Canton console
(for example because you are working directly against the topology gRPC APIs)
or need to compute the namespace yourself for other reasons. For those
cases, we document how to compute it in Python here:

lexicographic ordering on namespaces::

    def compute_decentralized_namespace(owners):
        builder = hashlib.sha256()
        # hash purpose prefix
        builder.update((37).to_bytes(4))
        for owner in sorted(owners):
            # namespace length
            builder.update(len(owner).to_bytes(4))
            builder.update(owner.encode("utf-8"))
        # 1220 is the Canton prefix for sha256 hashes
        return f"1220{builder.hexdigest()}"
