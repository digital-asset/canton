Mark a Protocol Version as Stable
=================================

In the notes below `ProtocolVersion.v36` (`v36`) is used as the protocol version that is being marked as stable.

- Create `v36` using `ProtocolVersion.createStable` and change the type to `ProtocolVersionWithStatus[ProtocolVersionAnnotation.Stable]`
- Remove `v36` from `ProtocolVersion.alpha` and add it to `ProtocolVersion.stable`.
- Check that `InterpretationConfig.forProtocolVersion` is associated with a stable interpretation config.
- All proto definitions that are associated with `v36` should be marked as stable in the proto files.
  - This is done by changing the `companion_extends` annotation from `AlphaProtoVersion` to `StableProtoVersion`
  - Any proto definitions used by a stable protocol version should be marked as stable.
- In `project/BuildCommon.scala` update the `community-base` build info key `stableProtocolVersions` to include `36` (no `v` prefix).
- In `ReleaseVersionToProtocolVersions.majorMinorToStableProtocolVersions` add `v36` to the all release versions that will support it.
- Add a note to `UNRELEASED.md` that the protocol version is now stable.

