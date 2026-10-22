Add a new protocol version
==========================

# Code and testing
- Remove coverage for the protocol version in CI files.
- Mark the protocol version as deleted (`ProtocolVersion.deleted`).
- Change `stableProtocolVersions` in `BuildCommon.scala`.
- Remove the protocol version from `majorMinorToStableProtocolVersions` for the corresponding release.

# Protobuf

For the example, suppose we are removing protocol version 35.
Consider the versioning tables (`VersioningTable` and `SupportedProtoVersions`):

- If it contains a single entry, bump 35 to 37.
- If it contains at least two entries but the first two are not consecutive protocol versions (e.g., 35
  and 37), then bump 35 to 36.
- Otherwise, the protobuf version is a candidate for removal.

About the removal of protobuf:

- Protocol messages are usually fine to remove.
- Be careful when removing a message and ensure it will not be needed.
- If removing a version allows to do clean up (e.g., merging a class into the parent trait), do it.
