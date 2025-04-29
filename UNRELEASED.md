# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## Until 2025-04-30 (Exclusive)
- JSON API - fixed openapi documentation for maps: (`eventsById`,`filtersByParty`).

### Changed return values in the console `grant` and `revoke` commands
The console commands `ledger_api.users.rights.grant` and `ledger_api.users.rights.revoke`
have been changed to return the complete state of current rights assigned to a user instead of
the "delta" induced by the command. The previous behavior was counterintuitive and was a source
of confusion that resulted in support tickets.

### BREAKING CHANGE: Per-synchronizer party allocation
Console commands and API endpoints for allocating/enabling and removing/disabling parties now operate on a per-synchronizer basis.
This means that party allocations must be done explicitly for each synchronizer, and that the participant
must be connected to each synchronizer at the time of enabling or disabling the party.

The console commands `participant.parties.enable` and `participant.parties.disable` have a new parameter `synchronizer: Option[SynchronizerAlias]`
that specifies on which synchronizer the party should be enabled or disabled. The parameter can be "omitted" or set to `None`, if the participant
is connected to only one synchronizer. The parameter `waitForSynchronizer: SynchronizerChoice` has been removed.

The console command `participant.ledger_api.parties.allocate` has a new parameter `synchronizer_id` for specifying the target synchronizer for the party allocation.
Similar to the parameter for the other console commands, this parameter can be omitted if the participant is connected to only one synchronizer.

The Ledger API request `PartyManagementService.AllocatePartyRequest` now has a new field `string synchronizer_id` for specifying the target synchronizer of the party allocation.
Similar to the parameter for the console commands, this parameter can be omitted if the participant is connected to only a one synchronizer.

If the synchronizer parameter is not specified and the participant is connected to multiple synchronizers, the request fails with the error `PARTY_ALLOCATION_CANNOT_DETERMINE_SYNCHRONIZER`.
If the participant is not connected to any synchronizer, the request fails with the error `PARTY_ALLOCATION_WITHOUT_CONNECTED_SYNCHRONIZER`.

The authorized store can still be used to store `PartyToParticipant` topology transactions, but users are discouraged from doing so.

## Until 2025-04-23 (Exclusive)
- The error code `ABORTED_DUE_TO_SHUTDOWN` is now used instead of the (duplicate) error code `SERVER_IS_SHUTTING_DOWN` that was previously used.

- JSON API - changed encoding for protobuf based enums.
Following types are now encoded as strings:

    - `HashingSchemeVersion`,
    - `PackageStatus`,
    - `ParticipantPermission`,
    - `SigningAlgorithmSpec`,
    - `SignatureFormat`,
    - `TransactionShape`,

- Canton console - ledger_api changed slightly:

    - `submit_assign`, `submit_unassign` and `submit_reassign` changed: the waitForParticipants removed as these
    endpoints now use the same synchronization mechanics as the transaction submission endpoints. Also the timeout
    field became optional: allowing to bypass synchronization if needed.
    - `submit_assign` and `submit_unassign` have the eventFormat parameter removed, and `submit_assign_with_format`
    and `submit_unassign_with_format` endpoints introduced to provide full functionality with the compromise that the
    result can be empty.

- A default value is provided for the ``transaction_format`` field inside of ``SubmitAndWaitForTransactionRequest``.
  You can now omit this field in both grpc and json requests, and get behavior consistent with the 3.2 version of
 Canton. This means you will receive a flat transaction with event visibility dictated by all ``act_as`` and ``read_as`` parties.
