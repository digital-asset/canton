..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_externally_signed_transactions:

==============================================
Submit Externally Signed Transactions - Part 1
==============================================

This tutorial demonstrates how to submit Daml commands to a Canton ledger using an external private key for transaction authorization. Before proceeding, it is recommended to review the
:ref:`external signing overview <sdk_external_signing_overview>` to understand the concept of external signing.

The tutorial illustrates the external signing process using two external parties, ``Alice`` and ``Bob``,
leveraging the Ping Daml Template which is included by default in all participant nodes.

.. toggle::

    .. literalinclude:: CANTON/community/participant/src/main/daml/canton-builtin-admin-workflow-ping/Canton/Internal/Ping.daml

- In Part 1 ``Alice`` creates a ``Ping`` contract.
- In Part 2 ``Bob`` exercises the ``Respond`` choice on the contract and archives it.

.. important::

    This tutorial is for demo purposes.
    The code snippets should not be used directly in a production environment.

Prerequisites
=============

For simplicity, this tutorial assumes a minimal Canton setup consisting of one participant node connected to one synchronizer (which includes both a sequencer node and a mediator node).

.. tip::

    If you already have such an instance running or have completed the :ref:`onboarding tutorial <tutorial_onboard_external_party>`, proceed to the :ref:`Setup <canton_external_signing_tutorial_setup>` section.

Start Canton
------------

To obtain a Canton artifact refer to the :externalref:`getting started <canton-getting-started>` section.
First, navigate to the interactive submission example folder located at ``examples/08-interactive-submission`` in the Canton release artifact.

.. note::

    All commands in this tutorial are expected to be run from that folder.

From the artifact directory, start Canton using the command:

.. code-block::

  ../../bin/canton -c examples/08-interactive-submission/interactive-submission.conf --bootstrap examples/08-interactive-submission/bootstrap.canton

Once the Welcome to Canton message appears, you are ready to proceed.

.. _canton_external_signing_tutorial_setup:

Setup
-----

Navigate to the interactive submission example folder located at ``examples/08-interactive-submission`` in the Canton release artifact.

This tutorial demonstrates external signing with two external parties: Alice and Bob.
If you haven't onboarded an external party yet, refer to the :ref:`onboarding tutorial <tutorial_onboard_external_party>`.

To proceed, gather the following information:

- ``Alice``'s Party Id, protocol signing private key, and protocol signing public key fingerprint
- ``Bob``'s Party Id
- Synchronizer Id to which the participant is connected
- gRPC Ledger API endpoint

The Party IDs and key-related information should already be known from the onboarding tutorial.
To retrieve the participant and synchronizer IDs, as well as the gRPC Ledger API ports, run the following commands in the Canton console:

.. snippet:: external_signing_submission
    .. hidden:: bootstrap.synchronizer_local()
    .. success:: sequencer1.synchronizer_id.filterString
    .. success:: participant1.config.ledgerApi.address
    .. success:: participant1.config.ledgerApi.port.unwrap

For this tutorial, the following values will be used (replace them with actual values):

- ``Alice`` Party Id: ``alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e``
- ``Bob`` Party Id: ``bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a``
- Synchronizer Id: ``da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3``
- gRPC Ledger API endpoint: ``localhost:4001``

API
---

This tutorial interacts with the ``InteractiveSubmissionService``, a service available on the gRPC Ledger API of the participant node.

.. toggle::

      .. literalinclude:: CANTON/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/interactive/interactive_submission_service.proto

.. _canton_externally_signing_tutorial_python_instructions:

Python
------

It is recommended to use a dedicated python environment to avoid conflicting dependencies.
Considering using `venv <https://docs.python.org/3/library/venv.html>`_.

.. code-block:: bash

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Then run the setup script to generate the necessary python files to interact with Canton's gRPC interface:

.. code-block::

    ./setup.sh

Shell
-----

For a terminal-based approach, install the following tools:

- `grpcurl <https://github.com/fullstorydev/grpcurl>`_
- `openssl <https://www.openssl.org/>`_
- `jq <https://jqlang.org/>`_

1. Prepare the transaction
==========================

Transform ``Ledger Command`` into a ``Daml Transaction``.

.. tabs::

   .. group-tab:: Bash

      Request:

        .. code-block:: bash

            echo '{
              "user_id": "demo_app",
              "command_id": "f2ec4d8f-ccc1-402b-b278-7556fdd2b412",
              "act_as": ["alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"],
              "synchronizer_id": "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3",
              "commands": [
                {
                  "create": {
                    "template_id": {
                      "package_id": "#canton-builtin-admin-workflow-ping",
                      "module_name": "Canton.Internal.Ping",
                      "entity_name": "Ping"
                    },
                    "create_arguments": {
                      "record_id": null,
                      "fields": [
                        {
                            "label" :"id",
                            "value": { "text": "ping_id" }
                        },
                        {
                            "label" :"initiator",
                            "value": { "party": "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e" }
                        },
                        {
                            "label" :"responder",
                            "value": { "party": "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a" }
                        }
                      ]
                    }
                  }
                }
              ]
            }' > create_ping_prepare_request.json

            cat "create_ping_prepare_request.json" | grpcurl -emit-defaults -plaintext -d @ localhost:4001 com.daml.ledger.api.v2.interactive.InteractiveSubmissionService/PrepareSubmission > create_ping_prepare_response.json

      Record the response in ``create_ping_prepare_response.json`` to make it easier to submit the transaction afterwards.
      Now inspect the response with

      .. code-block:: bash

        cat create_ping_prepare_response.json

      .. toggle::

         .. code-block:: json

            {
              "prepared_transaction": {
                "transaction": {
                  "version": "2.1",
                  "roots": [
                    "0"
                  ],
                  "nodes": [
                    {
                      "node_id": "0",
                      "v1": {
                        "create": {
                          "lf_version": "2.1",
                          "contract_id": "004c3409aa2e8f8e22604d58ea6211f667df2bae4abc7984a95d76b3d120b8bd85",
                          "package_name": "canton-builtin-admin-workflow-ping",
                          "template_id": {
                            "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                            "moduleName": "Canton.Internal.Ping",
                            "entityName": "Ping"
                          },
                          "argument": {
                            "record": {
                              "recordId": {
                                "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                                "moduleName": "Canton.Internal.Ping",
                                "entityName": "Ping"
                              },
                              "fields": [
                                {
                                  "label": "id",
                                  "value": {
                                    "text": "ping_id"
                                  }
                                },
                                {
                                  "label": "initiator",
                                  "value": {
                                    "party": "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                                  }
                                },
                                {
                                  "label": "responder",
                                  "value": {
                                    "party": "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                                  }
                                }
                              ]
                            }
                          },
                          "signatories": [
                            "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                          ],
                          "stakeholders": [
                            "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e",
                            "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                          ]
                        }
                      }
                    }
                  ],
                  "node_seeds": [
                    {
                      "seed": "Gv8neKcoUyIvsa5vdfjUxwGQLGuJOUeVO3j26YB4vOQ="
                    }
                  ]
                },
                "metadata": {
                  "submitter_info": {
                    "act_as": [
                      "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                    ],
                    "command_id": "f2ec4d8f-ccc1-402b-b278-7556fdd2b412"
                  },
                  "synchronizer_id": "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3",
                  "transaction_uuid": "0c36b6ea-a5a8-40ee-8708-d47589f34db7",
                  "submission_time": "1739897973772660"
                }
              },
              "prepared_transaction_hash": "lafpRryDAe5lA8sBONBv0u2umlGKtnJXnhec/7AN+Ro=",
              "hashing_scheme_version": "HASHING_SCHEME_VERSION_V2"
            }

   .. group-tab:: Python

      .. important::

        Ensure you have followed the :ref:`setup instructions for Python <canton_externally_signing_tutorial_python_instructions>` before proceeding.

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :start-after: [Imports]
        :end-before: [Imports End]
        :caption: Imports

      .. code-block:: python
         :caption: Tutorial variables

         initiator = "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
         responder = "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
         synchronizer_id = "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3"
         user_id = "demo_app"
         party = "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
         lapi_port="4001"

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :start-after: [Define ping template]
        :end-before: [Defined ping template]
        :caption: Define the ping template identifier

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :start-after: [Create LAPI gRPC Channel]
        :end-before: [Created LAPI gRPC Channel]
        :caption: Create gRPC clients

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :pyobject: prepare_create_ping_contract
        :caption: Prepare Create Ping Contract Function

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
         :dedent: 4
         :start-after: [Call the PrepareSubmission RPC]
         :end-before: [Transaction prepared]
         :caption: Call the prepare endpoint

Request
-------

- ``user_id``: Identifier for the application interacting with the ledger.
- ``command_id``: Unique, random string identifying this specific command. Each command submission must have a new and unique ``command_id``.
- ``act_as``: ID of the party issuing the command.
- ``synchronizer_id``: ID of the synchronizer that processes the transaction upon submission.
- ``commands``:  Ledger commands for submission. In this case, it shows the creation of a Ping contract with ``Alice`` as the initiator, ``Bob`` as the responder, and a ``ping_id`` value. See the `command documentation <https://docs.daml.com/app-dev/grpc/proto-docs.html#command-message-version-com-daml-ledger-api-v1>`_ for details.

Response
--------

Transaction
^^^^^^^^^^^

Represents the explicit ledger changes upon successful commitment.

- ``version``: Version of the transaction. This is also called ``LF Version``.
- ``roots``: List of root node ids. A Daml transaction is a list of trees. The nodes are flattened in a single list (see below). The root node ids design the root node of each individual tree in the transaction.

.. note::

 A current limitation of externally signed transactions is that they can only contain a single root node, and therefore a single transaction tree.

- ``nodes``: List of all nodes in the transaction. There are 4 types of nodes: ``Create``, ``Exercise``, ``Fetch`` and ``Rollback``.
  The number, type and content of each node depends on the Daml model and the state of the ledger.
- ``node_seeds``: List of seeds used by the Canton protocol to generate cryptographically secure salts. They can be ignored.

Metadata
^^^^^^^^

Additional information required for transaction processing.

- ``ledger_effective_time``: Time picked during the interpretation of the command into a transaction.
  Set if and only if the daml model makes use of time.
- ``submitter_info``: Contains the ``act_as`` party and ``command_id``
- ``synchronizer_id``: Synchronizer that will be used for transaction processing
- ``transaction_uuid``: Unique value generated by the prepare endpoint to uniquely identify this transaction

    - The transaction UUID is randomly selected during the ``prepare`` step and is fixed from that point forward. This allows the mediator node to de-duplicate transactions and prevent replays.
- ``mediator_group``: Group of mediators that will gather confirmation responses for the transaction. Can be ignored.
- ``submission_time``: The timestamp that the Canton protocol will use as a submission time to perform validations (e.g for de-duplication)
- ``disclosed_events``: Existing input contracts used in the transaction
- ``global_key_mapping``: Unused in the current version, can be ignored.

Hash
^^^^

- ``prepared_transaction_hash``: Pre-computed transaction hash. For security reasons the hash should be re-computed client-side as mentioned in the :ref:`Compute transaction hash <canton_recompute_external_transaction_hash>` section.

.. note::

   The ``prepare`` API can return additional details on how the Canton node is hashing the transaction to help troubleshoot hash-related errors (for example: pre-computed and re-computed hash mismatch).
    To enable it:

    1. Enable verbose hashing on the participant config ``ledger-api.interactive-submission-service.enable-verbose-hashing = true``.
    2. Set the ``verbose_hashing`` field in the ``PrepareSubmissionRequest`` to ``true``.

Hashing scheme version
^^^^^^^^^^^^^^^^^^^^^^

Version of the hashing scheme:

.. list-table::
   :widths: 10 15
   :header-rows: 1
   :align: center

   * - Protocol Version
     - Hashing Scheme Version
   * - 33
     - HASHING_SCHEME_VERSION_V2

.. note::

    If the gRPC Ledger API authorization is enabled, the user must have the ``readAs`` claim on behalf of ``Alice`` to call the ``prepare`` endpoint.

Traffic cost estimation
^^^^^^^^^^^^^^^^^^^^^^^

Estimates the traffic cost for the Participant Node that submits the transaction to the synchronizer.
See the :externalref:`API documentation <com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse.cost_estimation>` for details on the estimation response fields.

The precision of the estimation is influenced by several factors that cannot be known when the transaction is being prepared.
Additionally, the traffic cost will be incurred by the node submitting the transaction, not the one preparing it.
When they are the same, the cost estimation will be more accurate.
The following factors contribute to the uncertainty of the cost estimation:

    * Hosting relationship of the submitting party with the executing node
    * Number and type of external signatures provided upon submission of the transaction
    * Topology state of the network when the transaction will be submitted
    * Request amplification during submission
    * Part of the transaction that will be confirmed (root view or sub views)
    * Whether nodes approve or reject the transaction
    * IDs of contract created within the transaction are not suffixed in the cost estimation, whereas they are suffixed in the actual submission
    * Whether session signing keys are enabled on the submitting or confirming nodes

In most cases the impact of those factors is low and the variance they cause can generally be expected to be 10% or less.
Hints can be specified in the :externalref:`prepare request <com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.estimate_traffic_cost>` to help improve the precision of the estimation.

It's worth noting that :externalref:`request amplification <configure_request_amplification>` can significantly increase traffic cost when triggered.
The estimation provided is valid for a single confirmation request submission.
Subsequent amplified requests sent will cost additional traffic cost as they correspond to a new confirmation request being sent.

.. tip::

    Traffic cost estimation is enabled by default.
    To disable it, set the :externalref:`disabled <com.daml.ledger.api.v2.interactive.CostEstimationHints.disabled>` field in the ``CostEstimationHints`` message to ``true``.


For details on traffic management, read the related :externalref:`explanation <overview-explanation-traffic-management>` page.

.. _tutorial_external_signing_validate:

2. Validate the transaction
===========================

Deserialize and inspect the transaction to verify its correctness before proceeding.
The initiator of the transaction must be able to inspect and validate it to ensure it matches their intent before proceeding.
See the :externalref:`Trust Model <party_trust_model>` for guidance.

.. _canton_recompute_external_transaction_hash:

3. Compute the transaction hash
===============================

It is strongly recommended that the transaction hash be recomputed from the transaction and metadata to verify correctness.
The pre-computed hash provided in the ``Prepare`` step is for debugging purposes.

* The hashing algorithm specification is available :ref:`here <external_signing_hashing_algo>` as well as in the release artifact under ``protobuf/ledger-api/com/daml/ledger/api/v2/interactive/README.md``
* An example implementation in python is available in the release articact under ``examples/08-interactive-submission/daml_transaction_hashing_v2.py``

.. _canton_externally_sign_transaction:

4. Sign the transaction hash
============================

Using ``Alice``'s protocol signing private key, sign the hash.

.. note::

    Technically what is needed is the ability to sign with ``Alice``'s key, not the key itself.
    The management of the key can be delegated to a wallet, HSM or crypto custody provider. In this tutorial the
    key is managed locally and explicitly to demonstrate the signing process. Refer to the :ref:`onboarding tutorial <tutorial_onboard_external_party>`
    for details on how to generate a key for this tutorial.

.. tabs::

   .. group-tab:: Bash

        Assuming ``Alice's`` private key is stored in a file called ``alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e-private-key.der``, he hash can be signed using ``openssl``.

        .. note::

            In this tutorial the hash retrieved from the response of step 1 will be signed, without re-computing it as suggested in step 3.
            For an example of how to re-compute the hash, see the ``Python`` example.

        .. code-block:: bash

            TRANSACTION_HASH=$(cat create_ping_prepare_response.json | jq -r .prepared_transaction_hash)
            PREPARED_TRANSACTION=$(cat create_ping_prepare_response.json | jq -r .prepared_transaction)
            SIGNATURE=$(echo -n "$TRANSACTION_HASH" | base64 --decode | openssl pkeyutl -rawin -inkey alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e-private-key.der -keyform DER -sign | openssl base64 -e -A)

   .. group-tab:: Python

      The Python example demo includes an implementation of the transaction hashing algorithm.
      In this example, ``party_private_key`` is assumed to be an ``EllipticCurvePrivateKey`` Python object containing Alice's private key.
      If the :ref:`onboarding tutorial <tutorial_onboard_external_party>` was followed, this key should already be available.

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :dedent: 4
        :start-after: [Compute transaction hash]
        :end-before: [Signed hash]
        :caption: Re-compute hash and sign it

5. Execute the transaction
==========================

Submit the transaction and its signature to the ledger.

.. tabs::

   .. group-tab:: Bash

        .. code-block:: bash

            grpcurl -emit-defaults -plaintext -d @ localhost:4001 com.daml.ledger.api.v2.interactive.InteractiveSubmissionService/ExecuteSubmission <<EOM
                {
                  "prepared_transaction": $PREPARED_TRANSACTION,
                  "hashing_scheme_version": "HASHING_SCHEME_VERSION_V2",
                  "user_id": "demo_app",
                  "submission_id": "51dd5a0e-2ab6-4ca4-aa9d-9333fb603eb0",
                  "party_signatures": {
                    "signatures": [
                      {
                        "party": "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e",
                        "signatures": [
                          {
                            "format": "SIGNATURE_FORMAT_DER",
                            "signature": "$SIGNATURE",
                            "signing_algorithm_spec": "SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256",
                            "signed_by": "1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                          }
                        ]
                      }
                    ]
                  }
                }
                EOM

   .. group-tab:: Python

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
        :dedent: 4
        :start-after: [Signed hash]
        :end-before: [Submitted request]
        :caption: Submit the request to the ledger

In the request, note the presence of:

- ``submission_id``: Random string uniquely generated for this submission. This differs from the ``command_id`` in that a retry of this same prepared transaction would necessitate a new ``submission_id``. The ``submission_id`` is used to correlate several submissions of the same command with completion events (See :ref:`next step <tutorial_observe_externally_signed_transaction>` for more on completion events).

    - Because ``submission_id`` is not part of the signature, a command can be re-submitted with a different ``submission_id`` without requiring a new signature.

- ``signatures``: Object containing the signature of the transaction hash, along with metadata. In particular:

    - ``signing_algorithm_spec``: Will vary depending on the key used during onboarding.
    - ``signed_by``: Fingerprint of the protocol signing *public* key of ``Alice``. This tutorial assumes the same key was used to create ``Alice``'s namespace and her protocol signing key. This is why the fingerprint of the signing key matches the second part of her Party Id (after ``::``). For more details check out the :ref:`onboarding tutorial <tutorial_onboard_external_party>` and the `parties documentation <https://docs.daml.com/app-dev/parties-users.html#party-id-hints-and-display-names>`_.

.. note::

    If the gRPC Ledger API authorization is enabled, the user must have the ``actAs`` claim on behalf of ``Alice`` to call the ``execute`` endpoint.

.. _tutorial_observe_externally_signed_transaction:

6. Observe the transaction outcome
==================================

Monitor the completion stream for transaction confirmation, then retrieve the contract ID and binary blob representation.

.. tabs::

   .. group-tab:: Bash

        .. code-block:: bash

            grpcurl -emit-defaults -plaintext -d @ localhost:4001 com.daml.ledger.api.v2.CommandCompletionService/CompletionStream <<EOM
            {
              "user_id": "demo_app",
              "parties": ["alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"]
            }
            EOM

        Wait until observing a completion event:

        .. toggle::

            .. code-block:: json
               :emphasize-lines: 11

                {
                  "completion": {
                    "command_id": "f2ec4d8f-ccc1-402b-b278-7556fdd2b412",
                    "status": {
                      "code": 0,
                      "message": "",
                      "details": [

                      ]
                    },
                    "update_id": "122049bb312e4ba2e6f142b2221f58589b75b0ad253685d3fc82f5758686b037efdb",
                    "user_id": "demo_app",
                    "act_as": [
                      "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                    ],
                    "submission_id": "51dd5a0e-2ab6-4ca4-aa9d-9333fb603eb0",
                    "deduplication_offset": "0",
                    "trace_context": {
                      "traceparent": "00-65bc60e1399cad60cd2bceea6eddf4a7-9730a005a8779537-01"
                    },
                    "offset": "24",
                    "synchronizer_time": {
                      "synchronizer_id": "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3",
                      "record_time": "2025-02-19T17:21:50.512523Z"
                    }
                  }
                }


        - ``status.code``: A value of ``0`` indicates the command completed successfully
        - ``offset``: Ledger offset for the event
        - ``update_id``: Unique Id for this completion event
        - ``submission_id``: The submission Id chosen in the submission step

        Let's record both the ``offset`` and ``update_id`` for the next steps.

        .. code-block:: bash

            UPDATE_ID="122049bb312e4ba2e6f142b2221f58589b75b0ad253685d3fc82f5758686b037efdb"
            OFFSET="24"

        .. note::

            You may need to interrupt the command with ``Ctrl-C`` as the completion stream is a gRPC server streaming RPC which waits for updates from the server until interrupted.

        Let's now retrieve the corresponding transaction:

        .. code-block:: bash

            grpcurl -emit-defaults -plaintext -d @ localhost:4001 com.daml.ledger.api.v2.UpdateService/GetUpdateById <<EOM
            {
              "update_id": "$UPDATE_ID",
              "update_format": {
                "include_transactions": {
                  "event_format": {
                    "filters_by_party": {
                      "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e": {
                        "cumulative": [
                          {
                            "wildcard_filter": {
                              "include_created_event_blob": true
                            }
                          }
                        ]
                      }
                    }
                  },
                  "transaction_shape": TRANSACTION_SHAPE_ACS_DELTA
                }
              }
            }
            EOM

        .. toggle::

            .. code-block:: json

                {
                  "transaction": {
                    "update_id": "122049bb312e4ba2e6f142b2221f58589b75b0ad253685d3fc82f5758686b037efdb",
                    "command_id": "f2ec4d8f-ccc1-402p-b278-7556fdd2b412",
                    "workflow_id": "",
                    "effective_at": "2025-02-19T17:21:50.485967Z",
                    "events": [
                      {
                        "created": {
                          "offset": "24",
                          "node_id": 0,
                          "contract_id": "00aa1fb173904244c175e87ecc226ab652ecce76554c7f5700efd21d11484a2877ca101220a19a8de75c840e5063010386a811ca392e848441102749bf522cd394a816750a",
                          "template_id": {
                            "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                            "moduleName": "Canton.Internal.Ping",
                            "entityName": "Ping"
                          },
                          "contract_key": null,
                          "create_arguments": {
                            "recordId": {
                              "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                              "moduleName": "Canton.Internal.Ping",
                              "entityName": "Ping"
                            },
                            "fields": [
                              {
                                "label": "id",
                                "value": {
                                  "text": "ping_id"
                                }
                              },
                              {
                                "label": "initiator",
                                "value": {
                                  "party": "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                                }
                              },
                              {
                                "label": "responder",
                                "value": {
                                  "party": "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                                }
                              }
                            ]
                          },
                          "created_event_blob": "",
                          "interface_views": [

                          ],
                          "witness_parties": [
                            "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                          ],
                          "signatories": [
                            "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                          ],
                          "observers": [
                            "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                          ],
                          "created_at": "2025-02-19T17:21:50.485967Z",
                          "package_name": "canton-builtin-admin-workflow-ping"
                        }
                      }
                    ],
                    "offset": "24",
                    "synchronizer_id": "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3",
                    "trace_context": {
                      "traceparent": "00-65bc60e1399cad60cd2bceea6eddf4a7-9730a005a8779537-01"
                    },
                    "record_time": "2025-02-19T17:21:50.512523Z"
                  }
                }

        The ``events`` list includes a ``created object``, representing the newly created contract. Extract the corresponding ``contract_id`` for reference.
        To finalize this step, retrieve the binary blob representation of the created contract. This serialized form will be required when executing a choice on the contract in Part 2.

        .. code-block:: bash

            grpcurl -emit-defaults -plaintext -d @ localhost:4001 com.daml.ledger.api.v2.StateService/GetActiveContracts <<EOM
            {
              "event_format": {
                "filters_by_party": {
                  "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e": {
                    "cumulative": [
                      {
                        "wildcard_filter": {
                           "include_created_event_blob": true
                        }
                      }
                    ]
                  }
                }
              },
              "active_at_offset": "$OFFSET"
            }
            EOM

        .. toggle::

            .. code-block:: json

                {
                  "workflow_id": "",
                  "active_contract": {
                    "created_event": {
                      "offset": "24",
                      "node_id": 0,
                      "contract_id": "00aa1fb173904244c175e87ecc226ab652ecce76554c7f5700efd21d11484a2877ca101220a19a8de75c840e5063010386a811ca392e848441102749bf522cd394a816750a",
                      "template_id": {
                        "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                        "moduleName": "Canton.Internal.Ping",
                        "entityName": "Ping"
                      },
                      "contract_key": null,
                      "create_arguments": {
                        "recordId": {
                          "packageId": "9a19e9cc152538d3ad3b99b933ccf881e53b193ee6af17bdd9a65905a6e1f8ab",
                          "moduleName": "Canton.Internal.Ping",
                          "entityName": "Ping"
                        },
                        "fields": [
                          {
                            "label": "id",
                            "value": {
                              "text": "ping_id"
                            }
                          },
                          {
                            "label": "initiator",
                            "value": {
                              "party": "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                            }
                          },
                          {
                            "label": "responder",
                            "value": {
                              "party": "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                            }
                          }
                        ]
                      },
                      "created_event_blob": "CgMyLjESuQQKRQBjTUl2dyqzat8236jWAVQ7N5fKufP9tC25XfYKc8tcGMoQEiCC572aoJidPFPbnuK4QKF9wAuxGP1l7xumvhhNwM7khRIOQWRtaW5Xb3JrZmxvd3MaYApAOWExOWU5Y2MxNTI1MzhkM2FkM2I5OWI5MzNjY2Y4ODFlNTNiMTkzZWU2YWYxN2JkZDlhNjU5MDVhNmUxZjhhYhIGQ2FudG9uEghJbnRlcm5hbBIEUGluZxoEUGluZyKwAWqtAQoLCglCB3BpbmdfaWQKTwpNOkthbGljZTo6MTIyMGQ0NjZhNWQ5NmEzNTA5NzM2YzgyMWUyNWZlODFmYzhhNzNmMjI2ZDkyZTU3ZTk0YTY1MTcwZTU4YjA3ZmMwOGUKTQpLOklib2I6OjEyMjAyNTRkMDYwOTViNDA3ZjhjNmEzNzhiNmZjNDQzYTY3ZDMzNTZhYjhlZGZiZjEzNzhjYjNlNDQyMThkZTMyYzhhKkthbGljZTo6MTIyMGQ0NjZhNWQ5NmEzNTA5NzM2YzgyMWUyNWZlODFmYzhhNzNmMjI2ZDkyZTU3ZTk0YTY1MTcwZTU4YjA3ZmMwOGUySWJvYjo6MTIyMDI1NGQwNjA5NWI0MDdmOGM2YTM3OGI2ZmM0NDNhNjdkMzM1NmFiOGVkZmJmMTM3OGNiM2U0NDIxOGRlMzJjOGE544B3nIAuBgBCKgomCiQIARIgZrl+7TfHbM1LcYFnlh0pNxS091G09Le5mhD5PUCvwmkQHg==",
                      "interface_views": [

                      ],
                      "witness_parties": [
                        "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                      ],
                      "signatories": [
                        "alice::1220d466a5d96a3509736c821e25fe81fc8a73f226d92e57e94a65170e58b07fc08e"
                      ],
                      "observers": [
                        "bob::1220254d06095b407f8c6a378b6fc443a67d3356ab8edfbf1378cb3e44218de32c8a"
                      ],
                      "created_at": "2025-02-19T15:42:56.032995Z",
                      "package_name": "canton-builtin-admin-workflow-ping"
                    },
                    "synchronizer_id": "da::12203c0ecb446b35b0efa78e0bda9fd91716855866150a5eb7611a2ed5d418129de3",
                    "reassignment_counter": "0"
                  }
                }


        The request above might return multiple contracts if additional ones were created after the offset. The relevant contract should be identified by matching its ``contract_id``.
        The ``created_event_blob`` contains a serialized version of the contract, which can be used in subsequent transactions to exercise choices on it.

   .. group-tab:: Python

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
         :dedent: 4
         :start-after: [Waiting for the transaction to show on the completion stream]
         :end-before: [Got Contract Id from Transaction]
         :caption: Find completion event for the submission

      At this stage, the contract has been successfully created, and its ``contract_id`` is available.

      .. _tutorial_external_signing_execute_function:

      .. tip::

        The execution and extraction of the ``contract_id`` above are summarized in the following function, which is reused in Part 2 of the tutorial:

        .. toggle::

            .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
                :pyobject: execute_and_get_contract_id
                :caption: Execute submission and get contract id function


      To complete this part, the next step is to retrieve the binary blob of the creation event for the Ping contract.
      This serialized representation will be required in Part 2 when executing a choice on the contract.

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
         :pyobject: get_active_contracts
         :caption: Get active contracts function

      .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
         :dedent: 4
         :start-after: [Get created event from contract Id]
         :end-before: [Got created event from contract Id]
         :caption: Find completion event for the submission

This concludes Part 1 of the tutorial. In :ref:`Part 2 <tutorial_externally_signed_transactions_part_2>`, ``Bob`` exercises the ``Respond`` choice to archive the contract.
