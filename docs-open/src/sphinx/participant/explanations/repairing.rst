..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _repairing-explanation:

Repairing Participant Nodes
===========================

The Canton ledger is designed to self-heal and automatically recover from issues.
Therefore, in situations where degradation is anticipated, builtin mechanisms
support graceful degradation and automated recovery.

Common examples are database outages (retry until success) or network outages (failover
and reconnect until success).

Canton reports such issues as warnings to alert the operator about the degradation of
its dependencies, but does not require any manual intervention to recover from
a degradation.

However, not all situations are foreseeable, and system corruption can occur in
unexpected ways. Therefore, Canton supports manual repair. Regardless of the type of corruption,
a series of operational steps can be applied to repair a Participant Node to the correct
state. If multiple Participant Nodes in the distributed system are affected, coordinating
recovery across those Participant Nodes may be necessary.

Conceptually, Canton recovery is structured along the four layers:

  1. Participant Nodes have built-in and automated self-recovery and self-healing.
  2. After a restart or a crash, Participant Nodes automatically recover by re-creating a
     consistent state from the persisted store.
  3. After an outage of a Participant Node or of infrastructure that Participant Nodes depend
     on, there are industry-standard recovery procedures. Standard disaster recovery involves

     - manually restoring the database from a backup in case of a database outage and
     - automatically replaying messages from one or more Synchronizers, as the Participant Node
       reconnects to Synchronizers.

  4. In case of Participant Node corruption and active contract set (ACS) inconsistencies among
     multiple Participant Nodes, advanced manual repair and other console commands exist to
     re-establish a consistent state among Participant Nodes.

Repairing Participant Nodes is dangerous and complex. Therefore, you are discouraged from
attempting to repair Participant Nodes on your own unless you have expert knowledge and
experience. Instead, you are strongly advised to only repair Participant Nodes with the help
of technical support.

Best practices
--------------

If you run into corruption issues, you need to first understand what caused the issue.
Ideally, you rely on expert help to diagnose the issue and provide you with a custom recipe
on how to recover from the corruption issue and prevent recurrence.

The toolbox available consists of:

- Exporting and importing secret keys unless keys are stored externally in a Key Management Service (KMS)
- Manually initializing nodes
- Exporting and importing DARs
- Exporting and importing topology transactions
- Manually adding or removing contracts from the active contract set (ACS)
- Moving contracts from one Synchronizer to another
- Manually ignoring faulty transactions (and then using `add` and `purge` to repair the ACS).

All these methods are very powerful but dangerous. You should not attempt to repair your nodes on
your own as you risk severe data corruption.

Keep in mind that the corruption may not have been discovered immediately. Thus, the corruption
may have spread through the APIs to the applications using the Participant Node.
Bringing Participant Nodes back into a consistent state with respect to the other Participant Nodes
in the distributed system can therefore render the application state inconsistent with respect to
the Participant Node state. Accordingly, the application should either re-initialize itself from
the repaired state or itself put in place tools to fix inconsistencies.

Repair macros
-------------

Some operations are combined as macros, which are a series of consecutive repair commands,
coded as a single command. While you are discouraged from using these commands on your own,
these macros are documented for the sake of completeness.
