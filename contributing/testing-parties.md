Testing Parties
===============

Parties can exist strictly within the namespace of (one of) their hosting participant node, and thereby delegate their authority to that node. Such parties are called local parties.
External parties however do not delegate their authority to any single node and must explicitly provide their approval by signing with a private key they only control.
This has a number of consequences, largely on how these parties submit Daml transactions and on their topology state.

We here only focus on what that means in terms of testing. The following describes the best practices to interact with parties
in tests such that both local and external parties can be covered by the test.

Note that all tooling details below works both for local and external parties.

## Creating Parties

To create a party in tests, use:

`participant.parties.testing.enable(...)`.

Note the **testing** part. This method has the same signature as `participant.parties.enable` which you may be familiar with,
but it will create either a local or an external party, depending on the implicit `PartyKind` in scope.
This `partyKind` is set to local party by default and can be overridden to external party via an environment variable, see [Running Tests](#running-tests).

e.g:

```scala
val alice: Party = participant.parties.testing.enable("alice", synchronizer = daId)
```

> [!IMPORTANT]
> Note the `Party` type ascription. The `testing.enable` methods returns a `Party`.
> `Party` is a sealed trait, with `PartyId` and `ExternalParty` as subtypes.
>
> It is important to not accidentally downcast the created party to a `PartyId` (which will compile).
> For that reason watch out for patterns such as
>
> ```scala
> var alice: PartyId = _
> alice = participant.parties.testing.enable("alice", synchronizer = daId)
> ```
> This is an anti-pattern that will likely cause the test to fail when alice is created as an external party


### Multi synchronizer

To enable the same party on a different synchronizer, use:

`participant.parties.testing.also_enable(existingParty, newSynchronizer)`

## Topology

Topology changes impacting external parties cannot be directly authorized by their hosting node, for the reason mentioned at the beginning of this page.

For that reason, changes to the `PartyToParticipant` mapping of a party **which requires the party's authority** must be done using one of these commands instead:

`party.topology.party_to_participant_mappings.propose`

`party.topology.party_to_participant_mappings.propose_delta`

You may recognize this pattern, as it is the same used to authorize changes using the authority of a node.
The difference is that using the command above, the authority of the party is used to authorize the change.
Note again that these command work transparently both for local and external parties.

## Running tests

By default, tests are run with local parties. To run the test with an external party, set the `CANTON_TEXT_EXTERNAL_PARTIES=true` environment variable in your test

External parties tests can also be run with a manual test on CircleCI called `external_parties_test`.
Additionally, they're run nightly on main.

## Unsupported features

Some features are not supported by external parties. Mainly:

- Multi party submissions
- Multi root node transactions

For tests that rely on such features, add the `onlyRunWithLocalParty (...)` tag to get the test ignored when external parties are enabled.
The `onlyRunWithLocalParty` methods takes a `UnsupportedExternalPartyTest` that links to an issue indicating the reason why this test cannot run with external parties yet.

See [here](https://github.com/DACH-NY/canton/blob/main/community/app/src/test/scala/com/digitalasset/canton/integration/tests/CommandDeduplicationIntegrationTest.scala#L331) for an example.

## Common issues

### Party as PartyId

The most common issues come from the `Party` being silently downcasted to a `PartyId`.
This can happen either explicitly (see above), or implicitly, via an implicit conversion from `Party` to `PartyId`.

If you encounter errors such as

```
Request failed for participant1.
  GrpcRequestRefusedByServer: NOT_FOUND/NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT(11,b1ac1e04): This participant cannot submit as the given submitter on any connected synchronizer
```

lookout for functions taking a `PartyId` as input, but that get provided with a `Party`. It's generally enough to change the signature of the function to `Party`.

### No appropriate signing key

Another error that can appear is

```
Request failed for participant1.
  GrpcRequestRefusedByServer: NOT_FOUND/TOPOLOGY_NO_APPROPRIATE_SIGNING_KEY_IN_STORE(11,da8612eb): Could not find an appropriate signing key to issue the topology transaction
```

This usually means the a `participant.topology.party_to_participant_mappings.propose(party, ...)` or similar is called where in fact the authority of the party is required.

Try to switch the `party` and the `participant`:

`party.topology.party_to_participant_mappings.propose(participant, ...)`
