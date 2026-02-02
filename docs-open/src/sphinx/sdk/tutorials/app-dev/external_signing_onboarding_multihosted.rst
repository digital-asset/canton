..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_onboard_external_multi_hosted:

===================================
Onboard Multi-Hosted External Party
===================================

This tutorial demonstrates how to onboard an **external party** using the Ledger API which is hosted on multiple validators.
It is a simple extension to the :ref:`onboard external party tutorial <tutorial_onboard_external_party_lapi>`.

Prerequisites
=============

First read through the :ref:`onboard external party tutorial <tutorial_onboard_external_party_lapi>`
to familiarize yourself with the onboarding steps.

.. important::

    This tutorial uses openssl to create keys on the file system, which is not secure for production use.

From the artifact directory, start Canton using the command:

.. code-block::

   # This file will be written by Canton on startup and contain the runtime allocated ports
   export CANTON_PORTS_FILE=external_party_onboarding_multi_hosted.json
  ./bin/canton -c examples/08-interactive-submission/interactive-submission.conf --bootstrap examples/08-interactive-submission/bootstrap.canton

.. tip::
    A runnable script ``external_party_onboarding.sh`` located in the ``examples/08-interactive-submission`` directory of the Canton artifact puts together the steps in this tutorial as an example.
    Run the script with

    .. code-block::

        ./examples/08-interactive-submission/external_party_onboarding.sh --multi-hosted

    from the same directory where you started Canton such that the script can find
    the ``canton_ports.json`` file which contains the port configuration of the running Canton instance, or
    invoke the script with the hostname and port of the Ledger API using the command line argument ``-p1 <host>:<port>``.
    Note that the script supports a few command line arguments, which you can see by inspecting the code.

    To obtain a Canton artifact refer to the :ref:`getting started <canton-getting-started>` section.

Multi-Hosted Onboarding
=======================

The onboarding steps are initially exactly the same as the ones for a :ref:`non multi-hosted party <tutorial_onboard_external_party_lapi_steps>`.
The only two differences are:

- The :ref:`Create the topology transaction <tutorial_onboard_external_party_lapi_generate_step>` step is modified to include the additional hosting nodes.
- There is an extra step where the additional hosting nodes approve the hosting relationship.

Initial party setup:

.. snippet:: party_management_multi_hosted
    .. hidden:: bootstrap.synchronizer_local()
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. shell(cwd=CANTON):: PARTICIPANT1=$(echo "localhost:"$(jq -r ".participant1.jsonApi" external_party_onboarding_multi_hosted.json))
    .. shell:: SYNCHRONIZER_ID=$(curl -sS -f -L ${PARTICIPANT1}/v2/state/connected-synchronizers | jq ".connectedSynchronizers[0].synchronizerId")
    .. shell:: openssl genpkey -algorithm ed25519 -outform DER -out private_key.der
    .. shell:: openssl pkey -in private_key.der -pubout -outform DER -out public_key.der 2> /dev/null
    .. shell:: PUBLIC_KEY_BASE64=$(base64 -w 0 -i public_key.der)

We'll multi-host the party on ``PARTICIPANT1`` and ``PARTICIPANT2``.

.. snippet:: party_management_multi_hosted
    .. shell(cwd=CANTON):: PARTICIPANT2=$(echo "localhost:"$(jq -r ".participant2.jsonApi" external_party_onboarding_multi_hosted.json))
    .. shell:: OTHER_PARTICIPANT_UIDS=$(curl -sS -f -L ${PARTICIPANT2}/v2/parties/participant-id | jq -r .participantId)

To each hosting node is given ``Confirmation`` or ``Observation`` permission on behalf of the party.
At least one of the hosting nodes must have ``Confirmation`` permission.
For details on the relationship between parties and hosting nodes as well as the hosting permissions, see :externalref:`this page <overview_canton_external_parties>`.

.. tabs::

   .. tab:: Confirmation

       .. snippet:: party_management_multi_hosted
            .. shell:: GENERATE=$(cat << EOF
            {
            "synchronizer" : $SYNCHRONIZER_ID,
            "partyHint" : "Alice",
            "publicKey" : {
            "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
            "keyData": "$PUBLIC_KEY_BASE64",
            "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
            },
            "otherConfirmingParticipantUids" : ["$OTHER_PARTICIPANT_UIDS"],
            "confirmationThreshold": 2,
            }
            EOF
            )

       .. important::

            Note the confirmation threshold is set to two in this example.
            This has :externalref:`implications <overview_canton_external_parties>` on security and availability.

   .. tab:: Observation

       .. snippet:: party_management_multi_hosted
            .. shell:: GENERATE=$(cat << EOF
            {
            "synchronizer" : $SYNCHRONIZER_ID,
            "partyHint" : "Alice",
            "publicKey" : {
            "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
            "keyData": "$PUBLIC_KEY_BASE64",
            "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
            },
            "observingParticipantUids" : ["$OTHER_PARTICIPANT_UIDS"]
            }
            EOF
            )


Notice the ``otherConfirmingParticipantUids`` and ``observingParticipantUids`` fields set respectively for confirming and observing permissions.

The rest of the onboarding process is similar to single node hosting.

.. snippet:: party_management_multi_hosted
    .. shell:: ONBOARDING_TX=$(curl -f -sS -d "$GENERATE" -H "Content-Type: application/json" \
        -X POST ${PARTICIPANT1}/v2/parties/external/generate-topology)
    .. shell:: PARTY_ID=$(echo $ONBOARDING_TX | jq -r .partyId)
    .. shell:: TRANSACTIONS=$(echo $ONBOARDING_TX | jq '.topologyTransactions | map({ transaction : .})')
    .. shell:: PUBLIC_KEY_FINGERPRINT=$(echo $ONBOARDING_TX | jq -r .publicKeyFingerprint)
    .. shell:: MULTI_HASH=$(echo $ONBOARDING_TX | jq -r .multiHash)
    .. shell:: echo $MULTI_HASH | base64 --decode > hash_binary.bin
    .. shell:: openssl pkeyutl -sign -inkey private_key.der -rawin -in hash_binary.bin -out signature.bin -keyform DER
    .. shell:: SIGNATURE=$(base64 -w 0 < signature.bin)
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
    .. shell:: curl -sS -f -d "$ALLOCATE" -H "Content-Type: application/json" \
        -X POST ${PARTICIPANT1}/v2/parties/external/allocate

.. _tutorial_onboard_external_party_lapi_multi_hosted_pn_approves:

Finally, the last step required is to call the allocate endpoint on Participant 2 as well.

.. snippet:: party_management_multi_hosted
    .. shell:: curl -sS -f -d "$ALLOCATE" -H "Content-Type: application/json" \
        -X POST ${PARTICIPANT2}/v2/parties/external/allocate

When a party to participant mapping is uploaded through the allocate endpoint which mentions the local validator,
it will automatically be signed by the local validator and forwarded to the network. If the topology transaction
is not fully authorized, which means that still some signatures are missing, it is treated as a proposal.

If the proposal already exists on the network, the new signatures are merged into the proposal and once enough
signatures are present, the topology transaction is accepted and added to the state. Because of this, the signature
of the external party can also be omitted when uploading the topology transaction to the second participant.

This process can be scaled up to any number of hosting nodes.

.. tip::

    The authorization of the node to host the party can also be :externalref:`performed <howto_external_parties>` by node operators through the Admin API.

When allocating multi-hosted parties, the ``allocate`` endpoint is asynchronous, meaning when it returns the party may not be allocated yet.
To know when the party is ready to be used, you can poll the ``/v2/parties`` endpoint filtering for the party id until it appears in the response.

.. snippet:: party_management_multi_hosted
    .. shell:: curl -sS -f ${PARTICIPANT1}/v2/parties/$PARTY_ID
    .. shell:: curl -sS -f ${PARTICIPANT2}/v2/parties/$PARTY_ID
