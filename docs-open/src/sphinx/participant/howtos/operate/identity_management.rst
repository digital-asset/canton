..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Split this entire page into its pieces for specific howtos.
    High-level explanation covered by overview site.

.. _identity_management_user_manual:

Identity Management
===================
On-ledger identity management focuses on the distributed aspect of identities across Canton system entities, while
user identity management focuses on individual participants managing access of their users to their ledger APIs.

Canton comes with a built-in identity management system used to manage on-ledger identities. The technical details
are explained in the :externalref:`architecture section <identity-manager-1>`, while this write-up here is meant to give a high
level explanation.

The identity management system is self-contained and built without a trusted central entity or pre-defined root
certificate such that anyone can connect with anyone, without the need for some central approval and without the
danger of losing self-sovereignty.

Introduction
------------
What is a Canton Identity?
~~~~~~~~~~~~~~~~~~~~~~~~~~
When two system entities such as a participant, synchronizer topology manager, mediator or sequencer communicate
with each other, they will use asymmetric cryptography to encrypt messages and sign message contents
such that only the recipient can decrypt the content, verify the authenticity of the message, or prove its origin.
Therefore, we need a method to uniquely identify the system entities and a way to associate encryption and signing keys
with them.

On top of that, Canton uses the contract language Daml, which represents contract ownership and rights
through `parties <https://docs.daml.com/concepts/glossary.html#party>`_. But parties are not primary members
of the Canton synchronization protocol. They are represented by participants and therefore we need to
uniquely identify parties and relate them to participants, such that a participant can represent several
parties (and in Canton, a party can be represented by several participants).

Unique Identifier
~~~~~~~~~~~~~~~~~
A Canton identity is built out of two components: a random string ``X`` and a fingerprint of a public key ``N``.
This combination, ``(X,N)``, is called a *unique identifier* and is assumed to be globally unique by design.
This unique identifier is used in Canton to refer to particular parties, participants, or synchronizer entities.
A system entity (such as a party) is described by the combination of role (party, participant, mediator, sequencer,
synchronizer topology manager) and its unique identifier.

The system entities require knowledge about the keys that are used for encryption and signing by the
respective other entities.
This knowledge is distributed and therefore, the system entities require a way to verify that a certain
association of an entity with a key is correct and valid. This is the purpose of the fingerprint
of a public key in the unique identifier, which is referred to as *Namespace*. The secret key of the
corresponding namespace acts as the *root of trust* for that particular namespace, as explained later.

.. _identity-transactions:

Topology Transactions
~~~~~~~~~~~~~~~~~~~~~
In order to remain flexible and be able to change keys and cryptographic algorithms, we don't identify the
entities using a single static key, but we need a way to dynamically associate participants or synchronizer entities
with keys and parties with participants. We do this through topology transactions.

A topology transaction establishes a certain association of a unique identifier with either a key or a relationship
with another identifier. There are several different types of topology transactions. The most general one is the
``OwnerToKeyMapping``, which as the name says, :brokenref:`associates a key with a unique identifier <owner-to-key-mapping>`.
Such a topology transaction will inform all other system entities that a certain system entity is using a specific
key for a specific purpose, such as participant *Alice* of namespace *12345..* is using the key identified through
the fingerprint *AABBCCDDEE..* to sign messages.

Now, this poses two questions: who authorizes these transactions, and who distributes them?

For the authorization, we need to look at the second part of the unique identifier, the *Namespace*. A
topology transaction that refers to a particular unique identifier operates on that namespace and we require
that such a topology transaction is authorized by the corresponding secret key through a cryptographic
signature of the serialized topology transaction. This authorization can be either direct, if it is signed
by the secret key of the namespace, or indirect, if it is signed by a delegated key. To delegate the signing right to another key, there are other topology transactions of type *NamespaceDelegation* or
*IdentifierDelegation* that allow one to do that. A :brokenref:`namespace delegation <namespace-delegation>` delegates
entire namespaces to a certain key, such as saying the key identifier through the fingerprint *AABBCCDDEE...* is now
allowed to authorize topology transactions within the namespace of the key *VVWWXXYYZZ...*.

Signing of topology transactions happens in a ``TopologyManager``. Canton has many topology managers. Every
participant node and every synchronizer have topology managers with exactly the same functional capabilities, just different
impacts. They can create new keys, new namespaces, and the identity of new participants, parties, and synchronizers. And
they can export these topology transactions such that they can be imported by another topology manager. This allows you to
manage Canton identities in quite a wide range of ways. A participant can operate their own topology manager which
allows them individually to manage their parties. Or they can associate themselves with another topology manager and let them
manage the parties that they represent or keys they use. Or something in between, depending on the introduced
delegations and associations.

The difference between the synchronizer topology manager and the participant topology manager is that the synchronizer topology
manager establishes the valid topology state in a particular synchronizer by distributing topology transactions in a way that
every synchronizer member ends up with the same topology state. However, the synchronizer topology manager is just a gatekeeper of
the synchronizer that decides who is let in and who is not on that particular synchronizer, but the actual topology statements originate from
various sources. As such, the synchronizer topology manager can only block the distribution, but cannot fake topology
transactions.

The participant topology manager only manages an isolated topology state. However, there is a dispatcher attached to
this particular topology manager that attempts to register locally registered identities with remote synchronizers, by sending
them to the synchronizer topology managers, who then decide on whether they want to include them or not.

The careful reader will have noted that the described identity system indeed does not have a single root of trust or
decision maker on who is part of the overall system or not. But also that the topology state for the distributed
synchronization varies from synchronizer to synchronizer, allowing very flexible topologies and setups.

Legal Identities
~~~~~~~~~~~~~~~~
In Canton, we separate a system identity from the legal identity. While the above mechanism allows to
establish a common, verified and authorized knowledge of system entities, it doesn't guarantee that a
certain unique identifier really corresponds to a particular legal identity. Even more so, while the
unique identifier remains stable, a legal identity might change, for example in the case of a merger of
two companies. Therefore, Canton provides an administrative command which allows one to associate a randomized
system identity with a human readable *display name* using the ``participant.parties.set_display_name`` command.

..  note::
    A party display name is private to the participant. If such names should be shared among participants,
    we recommend to build a corresponding Daml workflow and some automation logic, listening to the
    results of the Daml workflow and updating the display name accordingly.

Life of a Party
~~~~~~~~~~~~~~~
In the tutorials, we use the ``participant.parties.enable("name")`` function to setup a party on a participant.
To understand the identity management system in Canton, it helps to look at the steps under the hood of how a new party
is added:

1. The ``participant.parties.enable`` function determines the unique identifier of the participant: ``participant.id``.
2. The party name is built as ``name::<namespace>``, where the ``namespace`` is the one of the participant.
3. A new party-to-participant mapping is authorized on the Admin API: ``participant.topology.party_to_participant_mappings.authorize(...)``
4. The ``ParticipantTopologyManager`` gets invoked by the GRPC request, creating a new ``SignedTopologyTransaction`` and
   tests whether the authorization can be added to the local topology state. If it can, the new topology transaction
   is added to the store.
5. The ``ParticipantTopologyDispatcher`` picks up the new transaction and requests the addition on all synchronizers via the
   ``RegisterTopologyTransactionRequest`` message sent to the topology manager through the sequencer.
6. A synchronizer receives this request and processes it according to the policy (open or permissioned). The default setting
   is open.
7. If approved, the request service attempts to add the new topology transaction to the ``SynchronizerTopologyManager``.
8. The ``SynchronizerTopologyManager`` checks whether the new topology transaction can be added to the synchronizer topology state. If
   yes, it gets written to the local topology store.
9. The ``ParticipantTopologyDispatcher`` picks up the new transaction and sends it to all participants (and back to itself)
   through the sequencer.
10. The sequencer timestamps the transaction and embeds it into the transaction stream.
11. The participants receive the transaction, verify the integrity and correctness against the topology state and add it
    to the state with the timestamp of the sequencer, such that everyone has a synchronous topology state.

Note that the ``participant.parties.enable`` macro only works if the participant controls their namespace themselves, either
directly by having the namespace key or through delegation (via ``NamespaceDelegation``).

.. TODO(i9579): adjust documentation in step 6 for closed synchronizers

Participant Onboarding
~~~~~~~~~~~~~~~~~~~~~~
Key to supporting topological flexibility is that participants can easily be added to new synchronizers. Therefore, the
on-boarding of new participants to synchronizers needs to be secure but convenient. Looking at the console command, we note
that in most examples, we are using the ``connect`` command to connect a participant to a synchronizer. The connect command
just wraps a set of admin-api commands:

.. literalinclude:: CANTON/community/app-base/src/main/scala/com/digitalasset/canton/console/commands/ParticipantAdministration.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: OnboardParticipantToConfig
   :end-before: architecture-handbook-entry-end: OnboardParticipantToConfig
   :dedent:

.. literalinclude:: CANTON/community/app-base/src/main/scala/com/digitalasset/canton/console/commands/ParticipantAdministration.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: OnboardParticipantConnect
   :end-before: architecture-handbook-entry-end: OnboardParticipantConnect
   :dedent:

We note that from a user perspective, all that needs to happen by default is to provide the connection information and
accept the terms of service (if required by the synchronizer) to set up a new synchronizer connection. There is no separate
onboarding step performed, no giant certificate signing exercise happens, everything is set up during the
first connection attempt. However, quite a few steps happen behind the scenes. Therefore, we briefly
summarise the process here step by step:

1. The administrator of an existing participant needs to invoke the ``synchronizers.connect_local`` command to add a new synchronizer.
   The mandatory arguments are a synchronizer *alias* (used internally to refer to a particular connection) and the
   sequencer connection URL (HTTP or HTTPS) including an optional port *http[s]://hostname[:port]/path*.
   Optional are a certificates path for a custom TLS certificate chain (otherwise the default jre root certificates
   are used) and the *synchronizer id* of a synchronizer. The *synchronizer id* is the unique identifier of the synchronizer that can
   be defined to prevent man-in-the-middle attacks (very similar to an SSH key fingerprint).

2. The participant opens a GRPC channel to the ``SequencerConnectService``.

3. The participant contacts the ``SequencerConnectService`` and checks if using the synchronizer requires signing
   specific terms of services. If required, the terms of service are displayed to the user and an approval is
   locally stored at the participant for later. If approved, the participant attempts to connect to the sequencer.

4. The participant verifies that the remote synchronizer is running a protocol version compatible with the participant's
   version using the ``SequencerConnectService.handshake``. If the participant runs an incompatible protocol version, the connection
   will fail.

5. The participant downloads and verifies the Synchronizer ID from the Synchronizer. The :brokenref:`Synchronizer ID <bootstrapping-idm>`
   can be used to verify the correct authorization of the topology transactions of the synchronizer entities.
   If the synchronizer id has been provided previously during the ``synchronizers.connect_local`` call (or in a previous session), the two
   IDs are compared. If they are not equal, the connection fails. If the synchronizer id was not provided during the
   ``synchronizers.connect_local`` call, the participant uses and stores the one downloaded. We assume here that the synchronizer id is
   obtained by the participant through a secure channel such that it is sure to be talking to the right synchronizer.
   Therefore, this secure channel can be either something happening outside of Canton or can be provided by TLS during
   the first time we contact a synchronizer.

6. The participant downloads the *static synchronizer parameters*, which are the parameters used for the transaction protocol
   on the particular synchronizer, such as the cryptographic keys supported by this synchronizer.

7. The participant connects to the sequencer initially as an unauthenticated member. Such members can only send
   transactions to the synchronizer topology manager. The participant then sends an initial set of topology transactions
   required to identify the participant and define the keys used by the participant to the ``SynchronizerTopologyManagerRequestService``.
   The request service inspects the validity of the transactions and decides based on the configured synchronizer on-boarding
   policy. The currently supported policies are ``open`` (default) and ``permissioned``.
   While ``open`` is convenient for permissionless systems and for development, it will accept any new participant and any topology transaction.
   The ``permissioned`` policy will accept the participant's onboarding transactions only if the participant has been
   added to the allow-list beforehand.

8. The request service forwards the transactions to the synchronizer topology manager, which attempts to add them to
   the state (and thus trigger the distribution to the other members on a synchronizer).
   The result of the onboarding request is sent to the unauthenticated member who disconnects upon receiving
   the response.

9. If the onboarding request is approved, the participant now attempts to connect to the sequencer as the actual
   participant.

10. Once the participant is properly enabled on the synchronizer and its signing key is known, the participant can subscribe
    to the ``SequencerService`` with its identity. To do that and to verify the authorization of any
    action on the ``SequencerService``, the participant must obtain an authorization token from the synchronizer.
    For this purpose, the participant requests a ``Challenge`` from the synchronizer. The synchronizer will provide it with a ``nonce``
    and the fingerprint of the key to be used for authentication. The participant signs this nonce
    (together with the synchronizer id) using the corresponding private key.
    The reason for the fingerprint is simple: the participant needs to sign the token using the participant's signing key
    as defined by the synchronizer topology state. However, as the participant will learn the true synchronizer topology state only
    by reading from the ``SequencerService``, it cannot know what the key is. Therefore, the synchronizer discloses this part
    of the synchronizer topology state as part of the authorization challenge.

11. Using the created authentication token, the participant starts to use the *SequencerService*. On the synchronizer side,
    the synchronizer verifies the authenticity and validity of the token by verifying that the token is the expected one and
    is signed by the participant's signing key. The token is used to authenticate every GRPC invocation and needs
    to be renewed regularly.

12. The participant sets up the ``ParticipantTopologyDispatcher``, which is the process that tries to push all topology transactions
    created at the participant node's topology manager to the synchronizer topology manager. If the participant is using its
    topology manager to manage its identity on its own, these transactions contain all the information about the
    registered parties or supported packages.

13. As mentioned above, the first set of messages received by the participant through the sequencer contains the
    synchronizer topology state, which includes the signing keys of the synchronizer entities. These messages are signed by the
    sequencer and topology manager and are self-consistent. If the participants know the synchronizer id, they can verify that
    they are talking to the expected synchronizer and that the keys of the synchronizer entities have been authorized by the owner of the
    key governing the synchronizer id.

14. Once the initial topology transactions have been read, the participant is ready to process transactions and send
    commands.

15. When a participant is (re-)enabled, the synchronizer topology dispatcher analyses the set of topology transactions the
    participant has missed before. It sends these transactions to the participant via the sequencer, before publicly
    enabling the participant. Therefore, when the participant starts to read messages from the sequencer, the
    initially received messages will be the topology state of the synchronizer.

Default Initialization
~~~~~~~~~~~~~~~~~~~~~~
The default initialization behavior of participant nodes and synchronizers is to run their own topology manager. This provides
a convenient, automatic way to configure the nodes and make them usable without manual intervention, but it can be
turned off by setting the ``auto-init = false`` configuration option **before** the first startup.

During the auto initialization, the following steps occur:

1. On the synchronizer, we generate four signing keys: one for the namespace and one each for the sequencer, mediator and
   topology manager. On the participant, we generate three keys: a namespace key, a signing key and an encryption key.

2. Using the fingerprint of the namespace, we generate the participant identity. For understandability, we use
   the node name used in the configuration file. This will change into a random identifier for privacy reasons.
   Once we've generated it, we set it using the ``set_id`` admin-api call.

3. We create a root certificate as ``NamespaceDelegation`` using the namespace key, signing with the namespace key.

4. Then, we create an ``OwnerToKeyMapping`` for the participant or synchronizer entities.

The `init.identity` object can be set to control the behavior of the auto initialization. For instance,
it is possible to control the identifier name that will be given to the node during the initialization.
There are 3 possible configurations:

1. Use the node name as the node identifier

.. literalinclude:: CANTON/community/app/src/test/resources/config-node-identifier.conf

2. Explicitly set a name

.. literalinclude:: CANTON/community/app/src/test/resources/explicit-node-identifier.conf

3. Generate a random name

.. literalinclude:: CANTON/community/app/src/test/resources/random-node-identifier.conf

Identity Setup Guide
~~~~~~~~~~~~~~~~~~~~
As explained, Canton nodes auto-initialize by default, running their own topology managers. This is
convenient for development and prototyping. Actual deployments require more care and therefore, this section should
serve as a brief guideline.

Canton topology managers have one crucial task they must not fail at: do not lose access to or control of the
root of trust (namespace keys). Any other key problem can somehow be recovered by revoking an old key
and issuing a new owner to key association. Therefore, it is advisable that participants and parties are associated
with a namespace managed by a topology manager that has sufficient operational setups to guarantee the security and
integrity of the namespace.

Therefore, a participant or synchronizer can

1. Run their own topology manager with their identity namespace key as part of the participant node.

2. Run their own topology manager on a detached computer in a self-built setup that exports topology transactions and
   transports them to the respective node (i.e. via burned CD roms).

3. Ask a trusted topology manager to issue a set of identifiers within the trusted topology manager's namespace
   as delegations and import the delegations to the local participant topology manager.

4. Let a trusted topology manager manage all the topology state on-behalf.

Obviously, there are more combinations and options possible, but these options here describe some common options
with different security and recoverability options.

To reduce the risk of losing namespace keys, additional keys can be created and allowed to operate on a
certain namespace. In fact, we recommend doing this and avoiding storing the root key on a live node.

User Identity Management
------------------------
So far we have covered how on-ledger identities are managed.

Every participant also needs to manage access to their local Ledger API and be able to give applications
permission to read or write to that API on behalf of parties.
While an on-ledger identity is represented as a party, an application on the Ledger API is represented and managed as a user.
A Ledger API server manages applications' identities through:

- authentication: recognizing which user an application corresponds to (essentially by matching an application name with a user name)
- authorization: knowing which rights an authenticated user has and restricting their Ledger API access according to those rights

Authentication is based on JWT and covered in the `application development/authorization section <https://docs.daml.com/app-dev/authorization.html>`_
of the manual; the related Ledger API authorization configuration is covered in the :ref:`Ledger API JWT configuration section <ledger-api-jwt-configuration>`.

Authorization is managed by the Ledger API's User Management Service.
In essence, a user is a mapping from a user name to a set of parties with read or write permissions.
In more detail a user consists of:

- a user ID (also called user name)
- an active/deactivated status (can be used to temporarily ban a user from accessing the Ledger API)
- an optional primary party (indicates which party to use by default when submitting a Ledger API command request as this user)
- a set of user rights (describes whether a user has access to the admin portion of the Ledger API and what parties this user can act or read as)
- a set of custom annotations (string-based key-value pairs, stored locally on the Ledger API server, that can be used to attach extra information to this party, e.g. how it relates to some business entity)

All these properties except the user ID can be modified.
To learn more about annotations refer to the :externalref:`Ledger API Reference documentation <com.daml.ledger.api.v2.admin.ObjectMeta>`.
For an overview of the Ledger API's UserManagementService, see this `section  <https://docs.daml.com/app-dev/services.html#user-management-service>`_.

You can manage users through the :ref:`Canton console user management commands <ledger_api.users.list>`, an alpha feature.
See the cookbook below for some concrete examples of how to manage users.

Cookbook
--------

Manage Users
~~~~~~~~~~~~~~~~~~~~~~

In this section, we present how you can manage participant users using the Canton console commands.
First, we create three parties that we'll use in subsequent examples:

.. snippet:: user_management
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. success:: val Seq(alice, bob, eve) = Seq("alice", "bob", "eve").map(p => participant1.parties.enable(name = p))

Create
^^^^^^

Next, create a user called ``myuser`` with act-as ``alice`` and read-as ``bob`` permissions and active user status. This user's primary party is ``alice``. The user is not an administrator
and has some custom annotations.

.. snippet:: user_management
    .. success:: val user = participant1.ledger_api.users.create(id = "myuser", actAs = Set(alice), readAs = Set(bob), primaryParty = Some(alice), participantAdmin = false, isDeactivated = false, annotations = Map("foo" -> "bar", "description" -> "This is a description"))
    .. assert:: user.id == "myuser"

There are some restrictions on what constitutes a valid annotation key. In contrast, the only constraint for annotation values is that they must not be empty.
To learn more about annotations refer to the :externalref:`Ledger API Reference documentation <com.daml.ledger.api.v2.admin.ObjectMeta>`.

Update
^^^^^^

You can update a user's primary party, active/deactivated status and annotations.
(You can also change what rights a user has, but using a different method presented further below.)

In the following snippet, you change the user's primary party to be unassigned,
leave the active/deactivated status intact,
and update the annotations.
In the annotations, you change the value of the ``description`` key, remove the ``foo`` key and add the new ``baz`` key.
The return value contains the updated state of the user:

.. snippet:: user_management
    .. success:: val updatedUser = participant1.ledger_api.users.update(id = user.id, modifier = user => { user.copy(primaryParty = None, annotations = user.annotations.updated("description", "This is a new description").removed("foo").updated("baz", "bar")) })
    .. assert:: updatedUser.id == "myuser"
    .. assert:: updatedUser.annotations == Map("description" -> "This is a new description", "baz" -> "bar")

You can also update the user's identity provider ID.
In the following snippets, you change the user's identity provider ID to the newly created one.
Note that originally the user belonged to the default identity provider whose id is represented as the empty string ```""```.

.. snippet:: user_management
    .. success:: participant1.ledger_api.identity_provider_config.create("idp-id1", isDeactivated = false, jwksUrl = "http://someurl", issuer = "issuer1", audience = None)
    .. success:: participant1.ledger_api.users.update_idp("myuser", sourceIdentityProviderId="", targetIdentityProviderId="idp-id1")
    .. success:: participant1.ledger_api.users.get("myuser", identityProviderId="idp-id1")

You can change the user's identity provider ID back to the default one:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.update_idp("myuser", sourceIdentityProviderId="idp-id1", targetIdentityProviderId="")
    .. success:: participant1.ledger_api.users.get("myuser", identityProviderId="")

Inspect
^^^^^^^

You can fetch the current state of the user as follows:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.get(user.id)


You can query what rights a user has:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.rights.list(user.id)

You can grant more rights.
The returned value contains only newly granted rights; it does not contain rights the user already had even if you attempted to grant them again (like the read-as ``alice`` right in this example):

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.rights.grant(id = user.id, actAs = Set(alice, bob), readAs = Set(eve), participantAdmin = true)

You can revoke rights from the user.
Again, the returned value contains only rights that were actually removed:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.rights.revoke(id = user.id, actAs = Set(bob), readAs = Set(alice), participantAdmin = true)

Now that you have granted and revoked some rights, you can fetch all of the user's rights again and see what they are:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.rights.list(user.id)

Also, multiple users can be fetched at the same time.
To do that, first create another user called ``myotheruser`` and then list all the users whose user name starts with ``my``:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.create(id = "myotheruser")
    .. success:: participant1.ledger_api.users.list(filterUser = "my")


Decommission
^^^^^^^^^^^^^^

You can delete a user by its ID:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.delete("myotheruser")

You can confirm it has been removed by e.g. listing it:

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.list("myotheruser")

If you want to prevent a user from accessing the Ledger API it may be better to deactivate it rather than deleting it. A deleted user can be recreated as if it never existed in the first place, while a deactivated user must be explicitly reactivated to be able to access the Ledger API again.

.. snippet:: user_management
    .. success:: participant1.ledger_api.users.update("myuser", user => user.copy(isDeactivated = true))

.. _canton-add-party-to-a-participant:


Configure a default Participant Admin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Fresh participant nodes come with a default participant admin user called ``participant_admin``, which
can be used to bootstrap other users.
You might prefer to have an admin user with a different user ID ready on a participant startup.
For such situations, you can specify an additional participant admin user with the user ID of your choice.

.. note:: If a user with the specified ID already exists, then no additional user will be created,
          even if the preexisting user was not an admin user.

.. code-block:: none
   :caption: additional-admin.conf

   canton.participants.myparticipant.ledger-api.user-management-service.additional-admin-user-id = "my-admin-id"


Adding a new Party to a Participant
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The simplest operation is adding a new party to a participant. For this, we add it normally at the topology manager
of the participant, which in the default case is part of the participant node. There is a simple macro to enable the
party on a given participant if the participant is running their own topology manager:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementIntegrationTest.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: EnableParty
   :end-before: architecture-handbook-entry-end: EnableParty
   :dedent:

This will create a new party in the namespace of the participant's topology manager.

And there is the corresponding disable macro:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementIntegrationTest.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: DisableParty
   :end-before: architecture-handbook-entry-end: DisableParty
   :dedent:

The macros themselves just use ``topology.party_to_participant_mappings.authorize`` to create the new party, but add some convenience such
as automatically determining the parameters for the ``authorize`` call.

.. note::
    Please note that the ``participant.parties.enable`` macro will add the parties to the same namespace as the participant is in.
    It only works if the participant has authority over that namespace either by possessing the root or a delegated key.

.. _separate-party-migration:

Client Controlled Party
~~~~~~~~~~~~~~~~~~~~~~~

Parties are only weakly tied to participant nodes. They can be allocated in their own namespace and then
delegated to a given participant. For simplicity and convenience, the participant creates new parties
in its own namespace by default, but there are situations where this is not desired.

A common scenario is that you first host the party on behalf of your client, but subsequently hand over
the party to the client's node. With the default party allocation, you would still control the party of the client.

To avoid this, you need your client to create a new party on their own and export a party delegation
to you. This party delegation can then be imported into your topology state, which will then allow you
to act on behalf of the party.

For this process, we use a participant node which won't be connected to any synchronizer. We don't need the full
node, but just the topology manager. First, we need to find out the participant ID of the hosting node:

.. snippet:: client_controlled_party
    .. hidden:: val synchronizerId = bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. hidden:: val client = participant1
    .. hidden:: val hosting = participant2
    .. success:: hosting.id.toProtoPrimitive

This identifier needs to be communicated to the client and can be imported using ``ParticipantId.tryFromProtoPrimitive``.
The client then creates first a new key (they could use the default key created):

.. snippet:: client_controlled_party
    .. success:: val secret = client.keys.secret.generate_signing_key("my-party-key", SigningKeyUsage.NamespaceOnly)

and an appropriate root certificate for this key:

.. snippet:: client_controlled_party
    .. success:: val rootCert = client.topology.namespace_delegations.propose_delegation(Namespace(secret.fingerprint), secret, CanSignAllMappings)

This root certificate needs to be exported into a file:

.. snippet:: client_controlled_party
    .. success:: import com.digitalasset.canton.util.BinaryFileUtil
    .. success:: rootCert.writeToFile("rootCert.bin")

Define the party ID of the party you want to create:

.. snippet:: client_controlled_party
    .. success:: val partyId = PartyId.tryCreate("Client", secret.fingerprint)

Create and export the party to participant delegation:

.. snippet:: client_controlled_party
    .. hidden:: val hostingNodeId = participant2.id
    .. success:: val partyDelegation = client.topology.party_to_participant_mappings.propose(partyId, Seq((hostingNodeId, ParticipantPermission.Submission)))
    .. success:: partyDelegation.writeToFile("partyDelegation.bin")

The client now shares the ``rootCert.bin`` and ``partyDelegation.bin`` files with the hosting node. The hosting
node imports them into their topology state:

.. snippet:: client_controlled_party
    .. success:: hosting.topology.transactions.load_single_from_files(
        files = Seq("rootCert.bin", "partyDelegation.bin"),
        store = synchronizerId,
    )

Finally, the hosting node needs to issue the corresponding topology transaction to enable the party on its node:

.. snippet:: client_controlled_party
    .. success:: hosting.topology.party_to_participant_mappings.propose(partyId, Seq((hosting.id, ParticipantPermission.Submission)))
    .. hidden:: utils.retry_until_true(
                  hosting.topology.party_to_participant_mappings
                     .list(synchronizerId.logical, filterParty=partyId.filterString)
                     .exists(_.item.participants.exists(_.participantId == hosting.id))
                )
    .. assert:: hosting.parties.hosted("Client").nonEmpty

Party on Multiple Nodes
~~~~~~~~~~~~~~~~~~~~~~~

:ref:`Hosting a party across multiple participants<multi-hosted-parties>`
increases its liveness and fault tolerance, as any hosting participant can act
on the party's behalf based on their :externalref:`permissions<topology-participant-permission>`.
While all hosting participants share the same view of the party's contracts,
they are also all included in its transactions. This creates overhead, limiting
the feature's scalability. For sharing data with many participants, explicit
disclosure is a more suitable approach.


.. _manually_initializing_node:

Manually Initializing a Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are situations where a node should not be automatically initialized, but where you should control
each step of the initialization. For example, this might be the case when a node in the setup does
not control its own identity, when you do not want to store the identity key on the node for security
reasons, or when you want to set our own keys (e.g. when keys are externally stored in a Key Management Service - KMS).

The following demonstrates the basic steps on how to initialize a node:

Keys Initialization
^^^^^^^^^^^^^^^^^^^

The following steps describe how to manually generate the necessary Canton keys (e.g. for a participant):

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementHelper.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: ManualInitKeys
   :end-before: architecture-handbook-entry-end: ManualInitKeys
   :dedent:

.. note::
   Be aware that in some particular use cases, you might want to register keys rather than generate new ones (for instance
   when you have pre-generated KMS keys that you want to use). Please refer to
   :ref:`External Key Storage with a Key Management Service (KMS) <external_key_storage>` for more details.

.. _manually-init-synchronizer:

Synchronizer Initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following steps describe how to manually initialize a synchronizer node:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementHelper.scala
    language: scala
    start-after: architecture-handbook-entry-begin: ManualInitSynchronizer
    end-before: architecture-handbook-entry-end: ManualInitSynchronizer
    dedent:

.. _manually-init-participant:

Participant Initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^
The following steps describe how to manually initialize a participant node:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementHelper.scala
    language: scala
    start-after: architecture-handbook-entry-begin: ManualInitParticipant
    end-before: architecture-handbook-entry-end: ManualInitParticipant
    dedent:
