..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_onboard_external_party:

==========================================
Onboard External Party Using the Admin API
==========================================

This tutorial demonstrates how to onboard an **external party** using the Admin API.
External parties can authorize Daml transactions without the need to trust any node of the network by signing transactions using a key they control.
Before proceeding, it is recommended to review the :ref:`external signing overview <sdk_external_signing_overview>` to understand the concept of external signing.
Additionally, the :ref:`topology tutorial <externally_signed_topology_transaction>` provides a detailed explanation of the topology concepts used in this tutorial.

The tutorial illustrates the onboarding of a party named ``Alice``. The process can be repeated any number of times to onboard new parties.

.. important::

    This tutorial is for demo purposes.
    The code snippets should not be used directly in a production environment.

Prerequisites
=============

For simplicity, this tutorial assumes a minimal Canton setup consisting of one participant node connected to one synchronizer (which includes both a sequencer node and a mediator node).

.. tip::

    If you already have such an instance running, proceed to the :ref:`Setup <tutorial_external_party_onboarding_setup>` section.

This configuration is not necessary to onboard external parties per se, but will be when submitting externally signed transactions.

Start Canton
------------

To obtain a Canton artifact refer to the :externalref:`getting started <canton-getting-started>` section.
From the artifact directory, start Canton using the command:

.. code-block::

  ./bin/canton -c examples/08-interactive-submission/interactive-submission.conf --bootstrap examples/08-interactive-submission/bootstrap.canton

Once the "Welcome to Canton" message appears, you are ready to proceed.

.. _tutorial_external_party_onboarding_setup:

Setup
-----

Navigate to the interactive submission example folder located at ``examples/08-interactive-submission`` in the Canton release artifact.

To proceed, gather the following information by running the commands below in the Canton console:

- Participant Id
- Admin API endpoint

.. snippet:: external_signing_onboarding
    .. success:: participant1.id.filterString
    .. success:: participant1.config.adminApi.address
    .. success:: participant1.config.adminApi.port.unwrap

In the rest of the tutorial we'll use the following values, but make sure to replace them with your own:

- Participant Id: ``participant1::122083aecbe5b3ca3c95c7584d2e0202891f8051d39754802a156521cd1677c8e759``
- Admin API endpoint: ``localhost:4002``

API
---

This tutorial interacts with the ``TopologyManagerWriteService``, a gRPC service available on the **Admin API** of the participant node.
See the :ref:`External Signing Topology Transaction Tutorial <canton_external_topology_manager_write_service>` for its definition.

.. _canton_external_party_onboarding_python_instructions:

It uses Python to demonstrate the onboarding of an external party.

It is recommended to use a dedicated python environment to avoid conflicting dependencies.
Considering using `venv <https://docs.python.org/3/library/venv.html>`_.

.. code-block:: bash

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Then run the setup script to generate the necessary python files to interact with Canton's gRPC interface:

.. code-block::

    ./setup.sh

.. important::

      The tutorial builds up on the :ref:`externally signed topology transactions tutorial <externally_signed_topology_transaction>` by re-using some if its code and concepts.
      For convenience, here are the topology utility functions used in the tutorial:

      .. toggle::

            .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py

Additionally, the following imports and variables are required for the rest of the tutorial:

.. toggle::

   .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
      :language: python
      :start-after: [Imports start]
      :end-before: [Imports end]
      :caption: Imports


   .. code-block:: python
      :caption: Tutorial variables

      admin_port="4002"

   .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
      :language: python
      :start-after: [Create Admin API gRPC Channel]
      :end-before: [Created Admin API gRPC Channel]
      :caption: gRPC channel

.. _external_signing_onboarding_topology_mappings:

Topology Mappings
=================

Onboarding an external party requires three topology mappings:

- ``NamespaceDelegation``: Defines a root namespace for the party and registers the **namespace signing** key, which is used to authorize topology changes involving the party's identity.
- ``PartyToKeyMapping``:

    - The **protocol signing** key responsible for authenticating the submission of Daml transactions to the ledger on behalf of the party.
    - A threshold (number) of keys, at least equal to the number of keys registered. At least threshold-many signatures must be obtained for a transaction submission to be authorized.

- ``PartyToParticipantMapping``:

    - Associates the party with one or more participant nodes, granting them **confirmation** rights. These rights allow participant nodes to validate Daml transactions involving the party and authorize their commitment to the ledger on behalf of the party.
    - A threshold (number) of participant node, at least equal to the number of hosting participants. At least threshold-many confirmations must be obtained from the hosting participants for a valid transaction to be authorized and committed to the ledger.

.. note::

    Hosting a party on more than one participant nodes for confirmation allows the party to reduce the trust put in any single node,
    as well as increase their overall availability on the network (e.g if a confirmation node becomes unavailable). See the :externalref:`Trust model <party_trust_model>` for more details.

Signing Keys
============

Canton uses digital signatures for authentication.
As shown in the previous section, two of the three required topology mappings, ``NamespaceDelegation`` and ``PartyToKeyMapping``, are used to register the corresponding public keys for these private keys.
Best practices suggest using separate signing keys for different purposes, and it is strongly recommended to use distinct key pairs for these two mappings. However, for simplicity, this tutorial will use a single key pair.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Generate a public/private key pair]
    :end-before: [Generated a public/private key pair]
    :caption: Generate a signing key pair
    :dedent: 4

Fingerprint
===========

Canton uses fingerprints to efficiently identify and reference signing keys.
Refer to the :ref:`Fingerprint <topology_transaction_fingerprint>` section of the topology tutorial for more information.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Compute the fingerprint of the public key]
    :end-before: [Computed the fingerprint of the public key]
    :caption: Compute the fingerprint
    :dedent: 4

Party ID
========

A ``Party ID`` is composed of two parts:

- A human readable name, in this case: ``alice``
- The fingerprint of the namespace signing key, also simply called ``namespace``

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Construct party ID]
    :end-before: [Constructed party ID]
    :caption: Construct the party ID
    :dedent: 4

.. _external_party_onboarding_transactions:

External Party Onboarding Transactions
======================================

Generate the three topology transactions necessary for the onboarding of ``Alice``.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :pyobject: build_serialized_transaction_and_hash
    :caption: Build and hash transaction function

.. _external_signing_onboarding_serial_update:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :pyobject: build_party_to_key_transaction
    :caption: Build a party to key transaction

.. note::

      The ``build_party_to_key_transaction`` function is an example of how to safely build a topology transaction by
      first obtaining the highest serial for it unique mapping, updating the mapping's content and incrementing the serial by 1.
      This ensures concurrent updates would be rejected. During onboarding of external parties however it is expected that
      there are no existing mappings and the serial will therefore bet set to 1.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Build onboarding transactions and their hash]
    :end-before: [Built onboarding transactions and their hash]
    :caption: Build and hash onboarding transactions
    :dedent: 4

This tutorial uses a single signing key, therefore all transactions are signed exclusively with that key (with the exception of the ``PartyToParticipant`` transaction that also needs to be signed by the hosting participant).
However, in a production environment where multiple keys are used, each transaction must be signed with the appropriate keys:

- ``Namespace Signing Key``: All transactions must be signed by this key, as it authorizes any topology state changes involving the party.
- ``PartyToKeyMapping`` Transaction: In addition to the namespace signing key, this transaction must be signed by all protocol signing keys it registers. This ensures the network can verify that the party has control over those keys.
- ``PartyToParticipantMapping`` Transaction: Along with the namespace signing key, this transaction must be authorized by all hosting participants it registers.

.. note::

      Any change to these topology transactions requires a signature from the namespace key.
      No node can alter the topology state of the external party without an explicit signature from its namespace key.

Multi Transaction Hash
======================

In order to reduce the number of signing operations required, compute a multi-transaction hash of all three transactions combined.
Signing this hash allows authenticating all three transactions at once.
A function to that effect is already available in the utility functions provided at the beginning of the tutorial.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Compute multi hash]
    :end-before: [Computed multi hash]
    :caption: Compute multi hash
    :dedent: 4

Signing
=======

First, sign the multi hash with the namespace key:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Sign multi hash]
    :end-before: [Signed multi hash]
    :caption: Sign multi hash
    :dedent: 4

Then, build the ``SignedTopologyTransaction`` messages expected by the Topology API:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :pyobject: build_signed_topology_transaction
    :caption: Build signed topology transaction function

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Build signed topology transactions]
    :end-before: [Built signed topology transactions]
    :dedent: 4

Submit
======

Submit the transactions signed with the external party's key:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :dedent: 4
    :start-after: [Load all three transactions onto the participant node]
    :end-before: [Loaded all three transactions onto the participant node]
    :caption: Load the signed transactions onto the participant
    :dedent: 4

Authorize PartyToParticipant Mapping
====================================

The hosting participant must authorize the PartyToParticipant transaction explicitly.
In this tutorial there's only one hosting participant, so its authorization is sufficient to complete the onboarding.
If there were multiple hosting participants for the marty, each would have to authorize the transaction individually.
See :externalref:`party replication <party-replication>` for more details.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :dedent: 4
    :start-after: [Authorize hosting from the confirming node]
    :end-before: [Authorized hosting from the confirming node]
    :caption: Authorize the PartyToParticipant transaction
    :dedent: 4

Observe Onboarded Party
=======================

Finally, wait to observe the party in the topology, confirming it was created successfully:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :pyobject: wait_to_observe_party_to_participant
    :caption: Observe PartyToParticipant transaction function

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_admin_api.py
    :language: python
    :start-after: [Waiting for party]
    :end-before: [Party found]
    :caption: Wait for party to appear in topology
    :dedent: 4

``Alice`` is now successfully onboarded and ready to interact with the ledger. Move to the next tutorial to learn how to :ref:`submit externally signed transactions <tutorial_externally_signed_transactions>`.

Tooling
=======

The scripts mentioned in this tutorial can be used as tools for testing and development purposes

Onboard external party
----------------------

Create an external party on the ledger and write their private and public keys to local `der` files.
By default the `synchronizer ID` and `participant ID` will be picked up from the files written by the canton bootstrap script in this directory.
They can be overridden with ` --synchronizer-id synchronizer_id` and `--participant-id participant_id`.

.. code-block:: bash

     ./setup.sh
     python interactive_submission.py create-party --name alice

Output:

.. code-block::

      Onboarding alice
      Waiting for alice to appear in topology
      Party ID: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d
      Written private key to: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d-private-key.der
      Written public key to: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d-public-key.der

Advanced Onboarding Topics
==========================

.. _external_party_multi_hosting:

Multi-Hosted Party
------------------

A multi hosted party is a party hosted on more than one Participant Node.
This tutorial uses a simplified setup with a single participant, however :externalref:`external parties can be multi-hosted <overview_canton_external_parties>`.

To create a multi-hosted external party, follow the tutorial above with the following two adjustments:

* Update the ``PartyToParticipant`` topology mapping:

      * List all hosting participants (along with their permission) instead of just one
      * Adjust the confirming threshold to strike the desired tradeoff between security and availability

* On each hosting participants, approve the ``PartyToParticipant`` transaction.

The following python function illustrates this process:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_multi_hosting.py
    :pyobject: authorize_external_party_hosting
    :caption: Function to lookup a pending PartyToParticipant transaction and authorize it

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_multi_hosting.py
    :pyobject: multi_host_party
    :caption: Function to onboard a new multi-hosted party

Example usage:

.. code-block:: Bash

      python external_party_onboarding_multi_hosting.py --admin-endpoint localhost:4012 localhost:4022 --synchronizer-id da::12204457ac942c4d839331d402f82ecc941c6232de06a88097ade653350a2d6fc9c5 --party-name charlie --threshold 1 onboard


.. _external_party_offline_replication:

Offline party replication
-------------------------

.. important::

      This section only illustrates how to authorize changes to the ``PartyToParticipant`` mapping of an external party.
      It is NOT sufficient to fully replicate an existing party to new nodes.
      Follow the procedure described in the :externalref:`offline party replication documentation <offline-party-replication>`.

Offline party replication is the action of replicating an existing party to additional hosting nodes.
This is a complex process described in detail in the :externalref:`offline party replication documentation <offline-party-replication>`.
The procedure is similar for local and external parties, with the exception that, as established in this tutorial, changes to the topology of the external party
need to be authorized explicitly with a signature of the topology transaction. Party replication involves updating the ``PartyToParticipant`` mapping of the party
and therefore signing the updated transaction with the namespace key of the party.

The following code demonstrates how to update the ``PartyToParticipant`` mapping for an external party via the Canton Admin API:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_multi_hosting.py
    :pyobject: update_party_to_participant_transaction
    :caption: Lookup the existing mapping and update it

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_multi_hosting.py
    :pyobject: update_external_party_hosting
    :caption: Authorize the new mapping

Example usage:

.. code-block:: Python

       python external_party_onboarding_multi_hosting.py --admin-endpoint localhost:4002 --synchronizer-id da::12204457ac942c4d839331d402f82ecc941c6232de06a88097ade653350a2d6fc9c5 --threshold 1 --private-key-file charlie::1220a844fb05224ef180032eb41c6ec9283f662beb1167ccb2d2fd9a4f67c0cc1529-private-key.der update --party-id charlie::1220a844fb05224ef180032eb41c6ec9283f662beb1167ccb2d2fd9a4f67c0cc1529 --participant-permission observation

For a complete example demonstrating external party multi-hosting, check out this file:

.. toggle::

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding_multi_hosting.py
          :caption: Multi-Hosted External party example
