..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _parties-and-users:

Parties and users on a Canton ledger
####################################

Identifying parties and users is an important part of building a workable Canton application. Recall these definitions from the :ref:`Get started with Canton and the JSON Ledger API <tutorial-canton-and-the-json-ledger-api>`:

- **Parties** are unique across the entire Canton network. These must be allocated before you can use them to log in, and allocation results in a random-looking (but not actually random) string that identifies the party and is used in your Daml code. Parties are a built-in concept.

- On each participant node, you can create **users** with human-readable user IDs. Each user can be associated with one or more parties allocated on that participant node, and refers to that party only on that node. Users are a purely local concept, meaning you can never address a user on another node by user ID, and you never work with users in your Daml code; party IDs are always used for these purposes. Users are also a built-in concept.

Parties in Daml SDK
*******************

When you allocate a party with a given hint Alice either in a Canton ledger you get back a party ID like ``Alice::1220f2fe29866fd6a0009ecc8a64ccdc09f1958bd0f801166baaee469d1251b2eb72``. The prefix before the double colon corresponds to the hint specified on party allocation. If the hint is not specified, it defaults to ``party-${randomUUID}``. The suffix is the fingerprint of the public key that can authorize topology transactions for this party.

.. note:: If you are working with a Canton sandbox, the keys are generated randomly. As a consequence the suffix will look different locally and every time you restart Sandbox and you will get a different party ID.

Party ID hints and attributes
*****************************

A party allocation is rejected if a party with the given hint already exists. The client can safely send the same request with the same hint, which will either allocate a party if the previous request failed or fail itself.

Party IDs are not human-readable, and the hints embedded in the prefixes are not universally unique. This makes associating parties with actual entities difficult. To alleviate this problem, we need a field or set of fields that describe a party in greater detail. The Ledger API contains a mechanism that allows associating with a party additional attributes in a dedicated ``object_metadata`` field.

Authorization and user management
*********************************

User management allows you to create users on a participant that are associated with a primary party and a dynamic set of actAs and readAs claims. Crucially, the user ID can be fully controlled when creating a user, unlike party IDs, and is unique on a single participant. You can also use the user ID in :subsiteref:`Access Tokens <authorization-claims>`. This means your IAM, which can sometimes be limited in configurability, only has to work with fixed user IDs.

However, users are purely local to a given participant. You cannot refer to users or parties associated with a given user on another participant via their user ID. You also need admin claims to interact with the user management endpoint for users other than your own. This means that while you can have a user ID in place of the primary party of your own user, you cannot generally replace party IDs with user IDs.

Working with parties
********************

So how do you handle the party IDs? The primary rule is to treat them as *opaque identifiers*. In particular, don’t parse them, don’t make assumptions about their format, and don’t try to turn arbitrary strings into party IDs. The only way to get a new party ID is as a result of a party allocation. Applications should never hardcode specific parties. Instead, either accept them as inputs or read them from the contract or choice arguments.

Daml script
===========

In Daml script, allocateParty returns the party ID that has been allocated. This party can then be used later, for example, in command submissions. When your script should refer to parties that have been allocated outside of the current script, accept those parties as arguments and pass them in via --input-file. Similarly, if your script allocates parties and you want to refer to them outside of the script, either in a later script or somewhere else, you can store them via --output-file. You can also query the party management and user management endpoints and get access to parties that way. Keep in mind, though, this requires admin rights on a participant, and there are no uniqueness guarantees for display names. That usually makes querying party and user management endpoints only an option for development, and we recommend passing parties as arguments where possible instead.

Java bindings
=============

When writing an application using the Java bindings, we recommend that you pass parties as arguments. They can either be CLI arguments or JVM properties.
