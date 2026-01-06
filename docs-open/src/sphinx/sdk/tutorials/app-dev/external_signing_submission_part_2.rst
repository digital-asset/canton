..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_externally_signed_transactions_part_2:

==============================================
Submit Externally Signed Transactions - Part 2
==============================================

Complete :ref:`Part 1 <tutorial_externally_signed_transactions>` before proceeding.

The tutorial illustrates the external signing process using two external parties, ``Alice`` and ``Bob``,
leveraging the same Ping Daml Template used in Part 1 of the tutorial.

- In Part 1 ``Alice`` created a ``Ping`` contract.
- In Part 2 ``Bob`` exercises the ``Respond`` choice on the contract and archives it.

The majority of the work involved in external transaction signing was completed in Part 1.
The key addition in Part 2 is utilizing the Ping contract created earlier through **explicit disclosure** and executing the Respond choice on that contract. The overall process remains similar to Part 1.

.. important::

    This tutorial is for demo purposes.
    The code snippets should not be used directly in a production environment.

Setup
=====

To proceed, gather the following information:

- ``Bob``'s Party ID, protocol signing private key, and protocol signing public key fingerprint
- Synchronizer ID of the synchronizer to which the participant is connected
- gRPC Ledger API endpoint
- ``ping_created_event``: Event retrieved in the last :ref:`step <tutorial_observe_externally_signed_transaction>` of Part 1.
- ``contract_id``: ID of the contract created in Part 1.

This information should already be known from the onboarding tutorial and the first part of the external signing tutorial.

Python
======

If you are following this tutorial in Python, generate gRPC Python classes by following the setup instructions in the ``README`` in the example folder.

Exercise ``Respond`` Choice
===========================

This tutorial does not repeat the material covered in Part 1 regarding transaction preparation, validation, signing, and execution, as these steps remain largely the same.
Instead, it highlights the key differences from Part 1.

Prepare the transaction
-----------------------

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
    :dedent: 4
    :start-after: [Create the exercise command]
    :end-before: [Created the exercise command]
    :caption: Create the exercise command

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
    :dedent: 4
    :start-after: [Prepare the exercise command]
    :end-before: [Prepared the exercise command]
    :caption: Prepare the exercise command

The ``Prepare`` request is very similar to the one from Part 1, with the following differences:

- ``act_as``: Now the ``responder``, ``Bob``, instead of the ``initiator``, ``Alice``. This makes sense because ``Bob`` is the one exercising the choice on the contract.
- ``commands``:  The command is now an ``Exercise``command instead of a ``Create`` command. Notably it requires the ``contract_id`` from Part 1.
- ``disclosed_contracts``: The serialized representation of contracts required to process the transaction.

Metadata
^^^^^^^^

The only significant difference with Part 1 in the metadata is: ``disclosed_events``.
This field now contains the input ``Ping`` contract. It is also included in the hash of the transaction.

.. important::

    Like in Part 1, the transaction must be :ref:`validated <tutorial_external_signing_validate>`, :ref:`hashed <canton_recompute_external_transaction_hash>` and :ref:`signed <canton_externally_sign_transaction>`.
    The hash computation and signature is performed by the ``execute_and_get_contract_id`` function provided :ref:`at the end of Part 1 <tutorial_external_signing_execute_function>`, as shown in the next section.

Submit and observe archived contract
------------------------------------

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_submission.py
    :dedent: 4
    :start-after: [Exercise the Respond choice on the ping contract by bob]
    :end-before: [Exercised Respond choice and observed archived contract]
    :caption: Submit the exercise transaction and observe the contract being archived


By querying the event service and filtering for the contract ID, an archived event is observed, confirming that the contract has been successfully archived.

This concludes the external signing tutorial.
The code used in this tutorial is available in the ``examples/08-interactive-submission`` folder and can be run with

.. code-block:: bash

     python interactive_submission.py run-demo

Tooling
=======

The scripts mentioned in this tutorial can be used as tools for testing and development purposes

Decode base64 encoded prepared transaction to JSON
--------------------------------------------------

.. code-block:: bash

     ./setup.sh
     python daml_transaction_util.py --decode --base64 <base64_encoded_transaction>

Compute hash of base64 encoded prepared transaction
---------------------------------------------------

.. code-block:: bash

     ./setup.sh
     python daml_transaction_util.py --hash --base64 <base64_encoded_transaction>
