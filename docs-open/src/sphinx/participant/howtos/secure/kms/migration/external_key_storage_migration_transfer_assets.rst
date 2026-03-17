..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_key_storage_migration_transfer_assets:

Migrate to external key storage with a KMS with asset transfer
==============================================================

In certain situations :ref:`importing private keys into a KMS <external_key_storage_migration_export_keys>` is not
possible or is deemed insecure, e.g. because the keys have remained unprotected until now and might be compromised.
In such cases, the only alternative is to create a new Participant with different keys, a different namespace, and
manually transfer all assets from the old Participant to the new. There are currently two scenarios we will explain: (1)
migrating a Participant with only external parties, and (2) migrating a participant that hosts *local* parties.

Participant with only external parties
--------------------------------------

:externalref:`External parties <overview_canton_external_parties>` are parties that exist outside of a Participant,
using their own keys and their own namespace. As such, any changes to the keys or namespace of the Participant
they are associated with do not affect their keys or any information in the contracts they own. Therefore, migrating
these *assets* to a new KMS-enabled Participant is relatively straightforward:

- First, start a new Participant with KMS by following this :ref:`guide <external_key_storage>`. Make sure you have created all the necessary keys and that the Participant starts correctly.

- Next, :ref:`replicate all the external parties <party-replication>` from the old Participant to the new KMS-enabled Participant. Once the process is complete and all parties have been replicated, you can decommission the old Participant.

Participant hosting local parties
---------------------------------

If a Participant hosts local parties, these parties, unlike external parties, share the same namespace and cryptographic
keys as the Participant they are hosted on. Therefore, any change to the Participant's keys will also change a
local party's identity, breaking its connection with its contracts.

The **only option for migration in this case is through model-specific transfers** from the old party to
the new party hosted in the new Participant running a KMS. This is a complex process and heavily depends on the specific
contracts and parties being migrated. If this is the only migration alternative, you will need to develop a migration
plan, including the correct DAML transactions, to ensure that contracts remain under the control of the new parties
hosted on the new Participant.

