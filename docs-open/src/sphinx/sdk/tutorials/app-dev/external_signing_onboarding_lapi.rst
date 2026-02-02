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

.. important::

    This tutorial uses openssl to create keys on the file system, which is not secure for production use.

From the artifact directory, start Canton using the command:

.. code-block::

  # This file will be written by Canton on startup and contain the runtime allocated ports
  export CANTON_PORTS_FILE=external_party_onboarding.json
  ./bin/canton -c examples/08-interactive-submission/interactive-submission.conf --bootstrap examples/08-interactive-submission/bootstrap.canton

.. tip::
    A runnable script ``external_party_onboarding.sh`` located in the ``examples/08-interactive-submission`` directory of the Canton artifact puts together the steps in this tutorial as an example.
    Run the script from the same directory where you started Canton such that the script can find
    the ``canton_ports.json`` file which contains the port configuration of the running Canton instance, or
    invoke the script with the hostname and port of the Ledger API using the command line argument ``-p1 <host>:<port>``.
    Note that the script supports a few command line arguments, which you can see by inspecting the code.

    To obtain a Canton artifact refer to the :ref:`getting started <canton-getting-started>` section.

.. _tutorial_onboard_external_party_lapi_steps:

Onboarding
==========

..
   _Note: When making changes to the onboarding steps in this file, make sure the script in
    community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh also reflects those changes

The onboarding steps are:

- Create a private key using openssl for the external party.
- Determine the available synchronizer-id.
- Create the topology transaction to define a new external party.
- Sign the topology transaction.
- Upload the signed topology transaction to the Ledger API.

First, determine the available synchronizer-ids using the ``v2/connected-synchronizers`` endpoint,
assuming that there is exactly one. The party allocation must be repeated for each synchronizer-id the party
should be hosted on.

Run this command from the same directory where you started Canton such that the script can find the ``canton_ports.json`` file.

.. snippet:: party_management
    .. hidden:: bootstrap.synchronizer_local()
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. shell(cwd=CANTON):: PARTICIPANT1=$(echo "localhost:"$(jq -r ".participant1.jsonApi" external_party_onboarding.json))
    .. shell:: SYNCHRONIZER_ID=$(curl -f -sS -L ${PARTICIPANT1}/v2/state/connected-synchronizers | jq ".connectedSynchronizers[0].synchronizerId")

Next, create a private Ed25519 key for the external party (other types of keys are supported as well).
The public key is then extracted in DER format and convert the binary DER format to base64.

.. snippet:: party_management
    .. shell:: openssl genpkey -algorithm ed25519 -outform DER -out private_key.der
    .. shell:: openssl pkey -in private_key.der -pubout -outform DER -out public_key.der 2> /dev/null
    .. shell:: PUBLIC_KEY_BASE64=$(base64 -w 0 -i public_key.der)

.. _tutorial_onboard_external_party_lapi_generate_step:

Use the convenience endpoint ``/v2/parties/external/generate-topology`` to generate the topology
transactions required to onboard the external party. This is fine if the node is trusted. In other scenarios,
the transactions should be built manually or inspected before signing, including recomputing the hash.

.. snippet:: party_management
    .. shell:: GENERATE=$(cat << EOF
    {
    "synchronizer" : $SYNCHRONIZER_ID,
    "partyHint" : "Alice",
    "publicKey" : {
    "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
    "keyData": "$PUBLIC_KEY_BASE64",
    "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
    },
    "otherConfirmingParticipantUids" : []
    }
    EOF
    )

.. snippet:: party_management
    .. shell:: ONBOARDING_TX=$(curl -f -sS -d "$GENERATE" -H "Content-Type: application/json" \
        -X POST ${PARTICIPANT1}/v2/parties/external/generate-topology)

The convenience endpoint returns the generated topology transactions together with the computed
party-id for the new party and the fingerprint of the public key. In addition, it also
returns a multi-hash, which is a commitment to the entire set of transactions.

.. snippet:: party_management
    .. shell:: PARTY_ID=$(echo $ONBOARDING_TX | jq -r .partyId)
    .. shell:: TRANSACTIONS=$(echo $ONBOARDING_TX | jq '.topologyTransactions | map({ transaction : .})')
    .. shell:: PUBLIC_KEY_FINGERPRINT=$(echo $ONBOARDING_TX | jq -r .publicKeyFingerprint)
    .. shell:: MULTI_HASH=$(echo $ONBOARDING_TX | jq -r .multiHash)

.. note::

    Do not make assumptions on the number of onboarding transactions returned by the endpoint as it is subject to change.
    To deserialize and inspect the transaction, checkout the external party topology transaction :ref:`tutorial <externally_signed_topology_transaction>`.

Sign the hash and convert the signature to base64:

.. snippet:: party_management
    .. shell:: echo $MULTI_HASH | base64 --decode > hash_binary.bin
    .. shell:: openssl pkeyutl -sign -inkey private_key.der -rawin -in hash_binary.bin -out signature.bin -keyform DER
    .. shell:: SIGNATURE=$(base64 -w 0 < signature.bin)

.. tip::
    The transactions can be signed one by one, or together as one hash, as done here.

Using the signature and the data from the previous step, submit the topology transactions and the
signature to the ledger API to complete the onboarding of the new external party:

.. snippet:: party_management
    .. shell:: ALLOCATE=$(cat << EOF
    {
    "synchronizer" : $SYNCHRONIZER_ID,
    "onboardingTransactions": $TRANSACTIONS,
    "multiHashSignatures": [
    {
    "format" : "SIGNATURE_FORMAT_CONCAT",
    "signature": "$SIGNATURE",
    "signedBy" : "$PUBLIC_KEY_FINGERPRINT",
    "signingAlgorithmSpec" : "SIGNING_ALGORITHM_SPEC_ED25519"
    }
    ]
    }
    EOF
    )

.. snippet:: party_management
    .. shell:: curl -f -sS -d "$ALLOCATE" -H "Content-Type: application/json" \
        -X POST ${PARTICIPANT1}/v2/parties/external/allocate

When allocating parties on a single node like here, the ``allocate`` endpoint is synchronous, meaning when it returns only when the party is allocated.
The party should now appear on the Ledger API:

.. snippet:: party_management
    .. shell:: curl -f -sS "${PARTICIPANT1}/v2/parties?filter-party=$PARTY_ID"
