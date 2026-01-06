..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _decentralization:

Decentralization
################

In the context of Canton, decentralization is primarily a distribution of power
or ownership over a particular entity to various owners or trustees. This means
that no trustee can unilaterally effect changes on behalf of that entity.

In Canton, such decentralized entities can be namespaces, parties, and
Synchronizers. See the section below for more details on the individual
decentralized entities.

A consequence of the distribution of ownership on one side, is the distribution
of trust on the other side. If an entity has a single owner, anybody interacting
with that entity must fully trust the owner or refrain from interacting with
that entity at all. When interacting with a decentralized entity, each
individual owner must only be trusted to the extent that is inversely
proportional to the number of owners required to reach consensus. This number is
called the *threshold* of the decentralized entity. The owners *reached a
quorum*, if at least threshold number of owners agreed to take the same action.

A few reasons, why decentralized trust can be useful or why it might even be a
requirement in some cases:

* **Security**: if the single owner of an entity gets compromised, the entire
  entity and all data associated with it gets compromised as well. In contrast,
  if one out of a total of six owners gets compromised, there are still five
  more functional owners that retain control over the entity. The compromised
  owner cannot make any changes unilaterally.
* **Regulatory restrictions**: One can imagine a restriction imposed by a
  regulator, that the Synchronizer must have physical presence in the
  jurisdication of the regulator. Another example of a regulatory requirement
  could be, that a network of national importance must be run by multiple
  independent public institutions and private companies to ensure independence
  of the network as a whole.
* **Four-eyes principle**: Setting the threshold to ``2`` enforces the owners to
  adhere to the four-eyes principle for a critical entity. This helps to guard
  against user input errors, for example.
* **Juridical person**: Multiple individual owners constitue a `juridical
  person <https://en.wikipedia.org/wiki/Juridical_person>`.

.. _decentralized-namespace:

Decentralized namespaces
************************

As described in the section :ref:`topology-namespaces`, a namespace is
ultimately backed by a private key. Whoever has access to the private key can
identify themselves as the owner of the namespace and act as such. Therefore, a
standard namespace is not suitable to represent a decentralized namespace.

Canton implements decentralized namespaces via the
``DecentralizedNamespaceDefinition`` topology mapping, which defines a set of
namespaces as owners and a ``Namespace`` for the decentralized namespace, which
is computed from the founders. Additionally, when defining the
``DecentralizedNamespaceDefinition``, the founders must agree on the threshold
they want to use.

The ownership of a decentralized namespace is not fixed. The current owners can
decide to add new owners, remove existing ones, or change the threshold of the
decentralized namespace. Changes in ownership after the initial topology
transaction does not change the ``Namespace`` of the decentralized namespace
itself.

Whenever a topology transaction expects a :ref:`unique identifier
<topology-unique-identifiers>`, the namespace for that unique identifier can be
a simple namespace defined by a ``NamespaceDelegation`` root certificate, or it
can be a decentralized namespace defined by a
``DecentralizedNamespaceDefinition``. The latter case requires that a quorum of
the owners sign subsequent topology transactions for the UID. Until the quorum
is reached, the topology transaction remains a proposal.

Decentralized Synchronizers
***************************

There are multiple ways a Synchronizer can be decentralized:

* **Decentralization of ownership**: the namespace for the ``SynchronizerId`` is
  created as a :ref:`decentralized namespace <decentralized-namespace>` with
  multiple owners and a threshold higher than ``1``.
* **Decentralization of Mediator groups**: the Synchronizer owners can set up
  Mediator groups with multiple mediators and a threshold higher than ``1``.
  This means that at least ``threshold`` number of Mediators in that group must
  reach the same verdict for a confirmation request in the :ref:`two-phase
  commit protocol <protocol-two-phase-commit>`, before the Sequencers deliver
  the verdict to the Participant Nodes.
* **Decentralization of Sequencers**: Canton supports running multiple
  Sequencers on top of a BFT ordering layer.
* **Decentralization of Sequencer connections**: Participant Nodes can further
  lower the trust put into single Sequencers by configuring the connection to
  the Synchronizer with multiple Sequencers. With the addition of setting a
  threshold to a higher number than ``1``, the Participant Node then subscribes
  to its event stream from multiple Sequencers. The Participant Node only
  processes events as long as ``threshold`` number of Sequencers have provided
  the same data for a single event, otherwise it raises a warning to notify the
  Participant Node's operator of a potential issue. Additionally, Participant
  Nodes may utilize a mechanism called *write amplification* to overcome
  censorship of faulty or malicious Sequencers, by submitting a message to
  multiple Sequencers. These duplicate messages get deduplicated by the
  Sequencers, so that eventually at most one message gets delivered to the
  recipients.

.. _decentralized-parties:

Decentralized parties
*********************

Decentralized ownership
-----------------------

A party's ownership and governance can be decentralized, just like the ownership
of a Synchronizer can be decentralized: by setting up a decentralized namespace
that is used for the party's UID.

Decentralized Active Contract management
----------------------------------------

From the perspective of a party, Active Contract management falls into three
functions, which are discussed below.

- Participant Nodes validating and confirming the transactions as part of the
  Canton Protocol
- The party's application submitting transactions via the Ledger API of a
  Participant Node
- The party's application reading the active contracts and the stream of
  transactions from the Ledger API of a Participant Node

Since a single entity operates a Participant Node, these functions can be
decentralized only by performing them on a quorum of Participant Nodes. So the
party is hosted on multiple Participant Nodes.

Decentralized validation
~~~~~~~~~~~~~~~~~~~~~~~~
The owner of a multi-hosted party can specify a threshold number for how
many Participant Nodes must confirm or reject each transaction on behalf of the
party. For example, let party `P` be hosted on three Participant Nodes `p1`,
`p2`, `p3` with threshold ``2``. Then, if a transaction requires
confirmation from this party, say because it is a signatory of a created or used
contract `c`, the Mediator waits for two of the three Participant Nodes to
send matching responses, either two confirmations or two rejections. When the
threshold is reached, the Mediator takes the corresponding response as the
response of the party. 

The Mediator issues the verdict on the transaction when no outstanding response
from any party could change it. For example, let the contract `c` have another
signatory `Q` hosted on four Participant Nodes `p5`, `p6`, `p7`, `p8` with
threshold ``2``. Then, both `P` and `Q` need to confirm the transaction. If
`P`'s response is a rejection, say because `p1` and `p2` sent rejections, the
Mediator immediately reaches the verdict “reject”. Conversely, if P's response
is a confirmation, the Mediator waits for two matching responses for `Q` from
`p5` to `p8` before it issues the verdict. For `Q`, it is actually possible that
both outcomes reach the threshold, say if `p5` and `p6` confirm and `p7` and
`p8` reject. In this case, the Mediator picks the outcome that reached the
threshold earlier.

The Mediator issues a timeout verdict if the threshold is not reached within the
participant-response-timeout for a party whose confirmations are needed. For
example, this happens for `P` if `p1` confirms, `p2` rejects, and `p3`
is overloaded and does not send anything in time.

Canton ensures that all Participant Nodes hosting the party are informed about
the transactions, independently of whether their confirmations were considered.
This ensures that they all emit the same transactions over the Ledger API and
update the set of active contracts accordingly.

The confirming Participant Nodes check, for example, that the contracts are
active (for signatories) and that the top-level actions are correctly authorized
(for controllers). Accordingly, any set of threshold-many colluding Participant
Nodes hosting the party has the following power:

- Confirm the activeness of an inactive contract and thereby spend a contract
  twice.
- Confirm the authority of the party and thereby create contracts with the party
  as a signatory even if the party has not previously agreed to being a
  signatory.

The latter in particular means that the threshold should be aligned with the
governance rules encoded in Daml models explicitly. In particular, if the
governance rules in the Daml model require a threshold of `t`, then the
confirmation threshold `ct` should also be at least `t`. Otherwise, `ct`
colluding Participant Node operators could simply agree to commit the effects of
a governance action without even going through the governance.

Submissions for decentralized parties
-------------------------------------

Parties with a threshold greater than ``1`` in the ``PartyToParticipant``
mapping cannot submit Ledger API commands. This is because command
submissions always go through a single Participant Node and there is no
cryptographic link between the party's submission and the resulting transaction.
Therefore, command submissions would violate the decentralization principle,
namely that a single entity, the Participant Node operator in this case, cannot
unilaterally change the active contracts of a decentralized party. 

There are two ways a party's owner can resolve this restriction:

1. Submit the transaction as externally signed submission
2. Decentralized authorization in Daml

The first option is the recommended approach.

External submissions
~~~~~~~~~~~~~~~~~~~~

The party's owners can authorize the topology mapping ``PartyToKeyMapping`` for
the party, which links the party to a set of signing keys. These signing keys
can be used by the owners to directly sign, and therefore authorize, a Daml
transaction before submitting it via a Participant Node. Such a submission is
called an *external submission*. Analogous to the threshold parameter in the
``PartyToParticipant`` topology mapping for defining the number of Participant
Nodes that need to come to the same verdict when validating a Daml Transaction,
the ``PartyToKeyMapping`` has a ``threshold`` parameter for defining the minimum
number of signing keys from topology mapping that must be used to sign the
submission for the submission to be authorized.

See :ref:`overview_canton_external_parties` for more details.


Decentralized authorization in Daml
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Coordinating a command submission across several Participant Nodes is a
distributed workflow and Daml is better suited to modeling this than encoding
fixed patterns in a protocol.

Therefore, a party `P` with a confirmation threshold greater than one
delegates its choices to (sufficiently many) other parties inside the Daml
model. Typically, the entities behind the party `P` have their own Daml
parties, and the contracts with `P` as a signatory defines choices with a
quorum of the entities' parties as controllers. This pattern mimics the idea
that a juridical person (`P`) acts through a quorum of natural persons (the
controllers).

There is one problem though: how can a party be in the signatory position of a
contract in the first place? This only works if the party submits a command that
creates the contract or if the party has previously created a different contract
that (transitively) delegates the right to do so.

To solve this problem, the party is initially allocated with a threshold of
``1`` on a single Participant Node. Then, the party submits the initial setup
commands through this Participant Node to create its “genesis state” in a Daml
contract. When the setup has succeeded, the party is decentralized by
distributing it over multiple Participant Nodes and increasing its threshold.
This approach has the drawback that the party's state can only evolve from the
genesis state in the predefined ways. For unforeseen changes, the party must be
centralized temporarily, unless the change can be expressed as Daml smart
contract upgrades. Such upgrades do not handle every possible case though. For
example, a major version upgrade of the Daml language itself is likely going to
be problematic.

Applications reading from multiple Participant Nodes
----------------------------------------------------
The third function is how the party's applications understand what is happening
on their behalf on the ledger. Most applications are operated by individuals and
they have a trust relationship with the operator of the Participant Node they
use. Thanks to this trust relationship, there is no need to question the
correctness of the data they receive from their Participant Node. In this
situation, it suffices to retrieve the data from a single participant.

If such a trust relationship is not given, say because the application is
operated by a group of entities, reading only from a single Participant Node
violates the decentralization principle, because this Participant Node could
serve wrong data. Such applications should therefore request their active
contract data from several Participant Nodes and check that they are consistent. 
