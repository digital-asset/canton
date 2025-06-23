# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## Until 2025-06-25 (Exclusive)
- Adds new gRPC endpoint `GetHighestOffsetByTimestamp` (and console command `find_highest_offset_by_timestamp`) that
  for a given timestamp, finds the highest ledger offset among all events that have record time <= timestamp. This is a
  backward-compatible change, because it's an addition only. It's useful for party replication / major upgrade.

## Until 2025-06-11 (Exclusive)
- Dead parameter `canton.participants.<participant>.http-ledger-api.allow-insecure-tokens` has been removed.

## Until 2025-06-04 (Exclusive)
- **Breaking** The console command `connect_local_bft` takes now a list of `SequencerReference` instead of a `NonEmpty[Map[SequencerAlias, SequencerReference]]`
- Console command - A new console command `connect_bft` has been added to connect by url to Decentralized Sequencers
-
## Until 2025-05-14 (Exclusive)
- JSON - changes in openapi (`Any` renamed as `ProtoAny`, `Event1` renamed to `TopologyEvent` and fixed, fixed `Field`, `FieldMask`,`JsReassignmentEvent` mappings.
- JSON API - fixed openapi documentation for maps: (`eventsById`,`filtersByParty`).
- JSON API - changed encoding for protobuf based enums.
  Following types are now encoded as strings:

    - `HashingSchemeVersion`,
    - `PackageStatus`,
    - `ParticipantPermission`,
    - `SigningAlgorithmSpec`,
    - `SignatureFormat`,
    - `TransactionShape`,

- Submission time is now called preparation time:
  - The field `submission_time` for interactive submissions is now called `preparation_time`.
  - The dynamic domain parameter `submission_time_record_time_tolerance` is now called `preparation_time_record_time_tolerance`.
  - The error codes `LOCAL_VERDICT_SUBMISSION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_SUBMISSION_TIME_TOLERANCE` are now called `LOCAL_VERDICT_PREPARATION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_PREPARATION_TIME_TOLERANCE`.
  - The console commands `set_submission_time_record_time_tolerance` is now called `set_preparation_time_record_time_tolerance`.
