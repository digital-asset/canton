Add a new protocol version
==========================

- Create the new version in `ProtocolVersion`.
- Add the version in the corresponding list (usually, `ProtocolVersion.alpha` for a new version).
- Add CI jobs for the new version. Typically, for a new protocol version with small payload:
  - Test the protocol version on main and release lines only (no need to run on each PR).
  - Add a manual job to test the new pv.
