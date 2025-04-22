# Release of Canton CANTON_VERSION

## Until 2025-04-23 (Exclusive)
- The error code `ABORTED_DUE_TO_SHUTDOWN` is now used instead of the (duplicate) error code `SERVER_IS_SHUTTING_DOWN` that was previously used.

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## Until 2025-04-23 (Exclusive)


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
