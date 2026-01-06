..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _sdk_external_signing_overview:

================
External Signing
================

External Signing Flow
=====================

This section describes how external signing flows at a high level, glossing over the technical details.
For an overview of external parties and how they differ from local parties, please read the corresponding :externalref:`overview section <overview_canton_external_parties>`.

Assume ``Alice`` is an external party already onboarded to the network.

You must take four steps to submit a transaction on behalf of ``Alice``

Step 1: Prepare the transaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, convert a **ledger command** into a **Daml transaction**.
This is traditionally handled by the submitting participant and does not involve the client application.
However, for an external submission you must obtain the full Daml transaction to authorize it.

Conceptually, a command is a high-level semantic intent to update the ledger: "Transfer 10 Canton Coins to Bob".
The corresponding transaction is a formal representation that explicitly details all ledger changes that would occur if the transaction is committed.
By signing the transaction, the external party indicates that it understands and authorizes all the effects of the command.

This step involves calling the ``Prepare Transaction API`` on a participant node, called the **Preparing Participant Node** (PPN), which takes a ledger command and returns a Daml transaction.

The PPN must:

- Be connected to the synchronizer that is going to execute the transaction.
- Have knowledge of the Daml packages used in the transaction (the relevant DARs must be uploaded and vetted).

.. note::

 The PPN does not need to host the submitting party.

Step 2: Sign the transaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have the transaction, sign it using ``Alice``'s private key to prove to the network that she agrees to the effects of this transaction on the ledger.

.. todo:: `#24019: Add link to transaction validation how-to <https://github.com/DACH-NY/canton/issues/24019>`_

In practice, the signature is performed on a hash of the transaction. The hash value is returned in the API response from the PPN along with the transaction.
However, you should compute the hash yourself from the transaction directly. Otherwise, the party may sign a different transaction given by a mismatching hash.
Instructions on how to compute the hash are available :ref:`here <external_signing_hashing_algo>`.

.. important::

 This is ``Alice``'s chance to validate the transaction and ensure it corresponds to her intent by inspecting its content.
 Additional documentation on how to validate the transaction is forthcoming.

Step 3: Submit the transaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Then, submit the transaction to the network by calling the `Execute Transaction API` on a participant node that takes the transaction and its signature.
This node is called the **Executing Participant Node** (EPN).

The EPN must:

- Be connected to the synchronizer on which the transaction is to be synchronized.

.. note::

 The EPN *may* be the same physical node as the PPN, but does not have to be.

**This completes the submission flow. The network then processes the transaction following the rules of the Canton protocol.**

Step 4: Observe the transaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The last step is to actually observe the result of the submission.
As mentioned earlier, external parties must be hosted on at least one participant node with confirmation rights, called nodes **Confirming Participant Nodes** (CPNs).
CPNs are responsible for validating the signature, the validity of the transaction, and recording the transaction on the party's ledger.

The CPNs must:

- Host ``Alice`` with confirmation rights.
- Be connected to the synchronizer on which the transaction is executed.
- Have knowledge of the Daml packages used in the transaction (the relevant DARs must be uploaded and vetted).

The client application reads the update stream on the CPNs and / or queries ``Alice``'s active contracts to get a hold of the eventual created contract(s).

.. note::

    Once again, one of the CPNs may be the same physical node as either the PPN or EPN, or both, but this is not a requirement.

.. note::

    Currently, the completion event stream that contains events informing the client application on the outcome of a command
    is only available on the EPN. This is an important consideration as completion events are the only reliable way for an application
    to know whether a command succeeded, and more importantly, if it failed, which may warrant a retry on the application's side.
    We intend to provide the completion event stream on CPNs as well in an upcoming Canton version.

.. important::

    External parties entrust CPNs with validating transactions on their behalf.
    See the :externalref:`Trust Model <party_trust_model>` for the implications and methods to mitigate the risks due to this trust assumption.

Additional resources
====================

Tutorials
^^^^^^^^^

Refer to the :ref:`external party onboarding tutorial <tutorial_onboard_external_party>` for a hands-on demonstration of onboarding an external party.
