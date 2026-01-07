..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _party-management:

Party Management
================

Parties are the entities that interact with the Daml ledger, representing users or organizations. They can be onboarded
to a Participant Node, which allows them to submit transactions and access the ledger.

The following section explains how to onboard a (local) party. Refer to the following howtos for onboarding of other kind of parties:

- For decentralized parties, refer to the :ref:`Decentralized Party Overview documentation <howto_decentralized_parties>`.
- For external parties, refer to the :externalref:`Onboard External Party tutorial <tutorial_onboard_external_party>`.
- For a party that is already hosted on a participant, refer to the :ref:`Party Replication documentation <party-replication>`.


.. snippet:: party_management
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "acme",
          sequencers = Seq(sequencer2),
          mediators = Seq(mediator2),
          synchronizerOwners = Seq(sequencer2),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: participant1.synchronizers.connect_local(sequencer2, "my-second-synchronizer")

Onboard a new party via the ledger API
--------------------------------------
If you have access to the ledger API, you can onboard a new party using the ``parties`` command.
This command is simply a wrapper around the underlying Ledger API endpoints.
For more information, see the :externalref:`Ledger API documentation <com.daml.ledger.api.v2.admin.PartyManagementService>`.

1. Define a name for the Party. You can choose the name freely, but it must conform to the following format: `[a-zA-Z0-9:\-_ ]`, must not
exceed 185 characters, must not use two consecutive colons, and must be unique in the namespace.

For example, we want to host the Party ``bob``.

.. snippet:: party_management
    .. success:: val bob = "bob"

2. Specify an optional Synchronizer ID to which the party should be allocated.
The participant must be connected to this Synchronizer. You may omit this parameter if the participant is connected to only one Synchronizer,
otherwise the party needs to be enabled on each synchronizer explicitly.

.. snippet:: party_management
    .. success:: val synchronizerId = participant1.synchronizers.id_of("my-synchronizer")

3. Define optional annotations. These are key-value pairs associated with this party and stored locally on this Ledger API server.
Annotations are useful for maintaining metadata about allocated parties.

.. snippet:: party_management
    .. success:: val annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3")

4. Define an optional identity provider id.

.. snippet:: party_management
    .. success:: val idpId = "idp-id-" + java.util.UUID.randomUUID().toString
    .. success:: participant1.ledger_api.identity_provider_config.create(identityProviderId = idpId, jwksUrl = "https://jwks:900", issuer = java.util.UUID.randomUUID().toString, audience = Option("someAudience"))

5. Enable the Party on this participant on ""my-synchronizer""

.. snippet:: party_management
    .. success:: participant1.ledger_api.parties.allocate(bob, annotations = annotations, identityProviderId = idpId, synchronizerId = Some(synchronizerId))

6. If you want to onboard the party on a second Synchronizer, you can do so by running the ``allocate`` command again with a different Synchronizer ID.

.. snippet:: party_management
    .. success:: val mySecondSynchronizerId = participant1.synchronizers.id_of("my-second-synchronizer")
    .. success:: participant1.ledger_api.parties.allocate(bob, annotations = annotations, identityProviderId = idpId, synchronizerId = Some(mySecondSynchronizerId))



Update a party
^^^^^^^^^^^^^^
1. You can update the annotations of a party. To do so, use the ``update`` command.

.. snippet:: party_management
    .. hidden:: val bobPartyId = participant1.ledger_api.parties.list().head.party
    .. success:: participant1.ledger_api.parties.update(bobPartyId, modifier = _.copy(annotations = Map("foo" -> "bar")), identityProviderId = idpId)

2. You can also update the identity provider of a party. To do so, use the ``update_idp`` command.

.. snippet:: party_management
    .. success:: participant1.ledger_api.parties.update_idp(bobPartyId, sourceIdentityProviderId = idpId, targetIdentityProviderId = "")

Find a party
^^^^^^^^^^^^
To find a party, you can use the ``list`` command.

1. You can filter parties by identity provider. Otherwise, all parties hosted on the participant will be returned.

.. snippet:: party_management
    .. success:: participant1.ledger_api.parties.list(idpId)


Onboard a new party via the admin API
--------------------------------------
If you need finer control when allocating a party, use the Admin API.
To onboard a new party to a Participant Node, follow these steps:

1. Define a name for the Party (same rules as explained above).
For example, we want to host the Party ``alice``.

.. snippet:: party_management
    .. success:: val alice = "alice"

2. Define an optional namespace. By default, Alice will use the namespace of the participant from whom you submit the command.

For more information on namespaces, refer to the :externalref:`Namespaces documentation <topology-namespaces>`.

3. Specify an optional Synchronizer alias to which the party should be allocated.
The participant must be connected to this Synchronizer. You may omit this parameter if the participant is connected to only one Synchronizer,
otherwise the party needs to be enabled on each synchronizer explicitly.

4. Enable the Party on this participant

.. snippet:: party_management
    .. success:: participant1.parties.enable(alice, synchronizer = Some("my-synchronizer"))


5. Verify that the party has been onboarded.

.. snippet:: party_management
    .. success:: participant1.parties.list("alice", filterParticipant = participant1.filterString)
    .. assert:: participant1.parties.list("alice", filterParticipant = participant1.filterString).nonEmpty

6. If you want to onboard the party on a second Synchronizer, you can do so by running the ``enable`` command again with a different Synchronizer alias.

.. snippet:: party_management
    .. success:: participant1.parties.enable("alice", synchronizer = Some("my-second-synchronizer"))


Find a party
^^^^^^^^^^^^
To find a party, you can use the ``list`` command.

.. snippet:: party_management
    .. success:: participant1.parties.list("alice")


You can also filter by Participant Node and Synchronizers.

.. snippet:: party_management
    .. success:: val synchronizerId = participant1.synchronizers.id_of("my-synchronizer")
    .. success:: participant1.parties.list("alice", filterParticipant = participant1.filterString, synchronizerIds = Set(synchronizerId))


Disable a party
^^^^^^^^^^^^^^^
.. warning::
    Disabling a party is not currently supported and is considered a dangerous operation.

If you are certain about what you are doing, you can disable a party on a specific Synchronizer using the following command:

.. snippet:: party_management
    .. hidden:: val alicePartyId = participant1.parties.find("alice")
    .. assert:: participant1.parties.list("alice", filterParticipant = participant1.filterString).nonEmpty
    .. success:: participant1.parties.disable(alicePartyId, synchronizer = Some("my-synchronizer"))
    .. success:: participant1.parties.disable(alicePartyId, synchronizer = Some("my-second-synchronizer"))
    .. assert:: participant1.parties.list("alice", filterParticipant = participant1.filterString).isEmpty


.. _multi-hosted-parties:

Multi-hosted parties
--------------------

A *multi-hosted party* is a party which is hosted on more than
one participant. This poses the question how you can :ref:`replicate a party<party-replication>`
from one participant to another?

The :ref:`simplest and safest way to multi-host a party<replicate-before-party-is-used>`
is only available to you while the party has not been involved in any Daml transaction.
Otherwise, you have to perform an :ref:`offline party replication<offline-party-replication>`
procedure.
