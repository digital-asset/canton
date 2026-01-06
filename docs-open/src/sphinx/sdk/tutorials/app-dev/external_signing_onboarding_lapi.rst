..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_onboard_external_party_lapi:

======================
Onboard External Party
======================

This tutorial demonstrates how to onboard an **external party** using the Ledger API.

Prerequisites
=============

This tutorial uses a script which is included as an example in the Canton artifact. Please note that the script uses
openssl to create keys on the file system, which is not secure for production use.

To obtain a Canton artifact refer to the :ref:`getting started <canton-getting-started>` section.
From the artifact directory, start Canton using the command:

.. code-block::

  ./bin/canton -c examples/08-interactive-submission/interactive-submission.conf --bootstrap examples/08-interactive-submission/bootstrap.canton

Run The Script
==============

The steps of this tutorial are included in the script ``external_party_onboarding.sh`` located in the ``examples/08-interactive-submission`` directory of the artifact. The steps covered by the script are:

- Create a private key using openssl for the external party.
- Determine the synchronizer-id available.
- Create a set of topology transactions to define a new external party.
- Sign the topology transactions.
- Upload the signed topology transactions to the Ledger API.

Make sure to run the script from the same directory where you started Canton such that the script can find
the ``canton_ports.json`` file which contains the port configuration of the running Canton instance, or
invoke the script with the hostname and port of the Ledger API using the command line argument ``-p1 <host>:<port>``.

Once you start it, you will see:

.. code-block::

    ./examples/08-interactive-submission/external_party_onboarding.sh
    Fetching localhost:7374/v2/state/connected-synchronizers
    Detected synchronizer-id "da::1220682ef8618b4425e8b1c5d7104260d5340eb4140509e99050a6bc9c5e8898d7b4"
    Requesting generate topology transactions
    Signing hash EiAfdSBLNQswwxUq9LyAYqHj8C5FzeZNLVvUJSgyrtORWg== for MyParty::1220ad82d8863893d65f10e2275a2f7b7af5c26cca97a761cb7cdc77d68e1ba20dc5 using ED25519
    Submitting onboarding transaction to participant1
    Onboarded party "MyParty::1220ad82d8863893d65f10e2275a2f7b7af5c26cca97a761cb7cdc77d68e1ba20dc5"

Note that the script supports a few command line arguments, which you can see by inspecting the code.

The Details of the Script
=========================

First, the script determines the available synchronizer-ids using the ``v2/connected-synchronizers`` endpoint,
assuming that there is exactly one. The party allocation must be repeated for each synchronizer-id the party
should be hosted on.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-find-synchronizer-id]
   :end-before: [end-external-party-find-synchronizer-id]

Next, openssl is used to create a private Ed25519 key for the external party (other types of keys are supported as well).
The public key is then extracted in DER format and convert the binary DER format to base64.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-generate-keys]
   :end-before: [end-external-party-generate-keys]

The script uses the convenience endpoint ``/v2/parties/external/generate-topology`` to generate the topology
transactions required to onboard the external party. This is fine if the node is trusted. In other scenarios,
the transactions should be built manually or inspected before signing, including recomputing the hash.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-generate-onboarding-transaction]
   :end-before: [end-external-party-generate-onboarding-transaction]

The convenience endpoint returns the generated topology transactions together with the computed
party-id for the new party and the fingerprint of the public key. In addition, it also
returns a multi-hash, which is a commitment to the entire set of transactions.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-extract-results]
   :end-before: [end-external-party-extract-results]

This hash needs to be signed by the private key of the new party. The script uses openssl to sign the hash and then
converts the signature to base64.

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-sign-multi-hash]
   :end-before: [end-external-party-sign-multi-hash]

Using the signature and the data from the previous step, the script submits the topology transactions and the
signature to the ledger API to complete the onboarding of the new external party:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-submit-onboarding-transaction]
   :end-before: [end-external-party-submit-onboarding-transaction]

The transactions can be signed one by one, or together as one hash, as done in the script.
