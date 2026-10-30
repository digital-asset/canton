# Ledger API size config

Size-limit configuration classes for the Ledger API requests, generated from the protos of
`ledger-api-proto` by the protoc plugin in `community/size-config-gen`.

- One `<Message>SizeConfig` class per message of `com.daml.ledger.api.v2`, `com.daml.ledger.api.v2.admin` and
  `com.daml.ledger.api.v2.interactive` that has a `map`, `repeated` or `bytes`
  field, or a field marked with `[(size_limit) = true]`.
- One root class per package: `PublicLapiSizeConfig`, `admin.AdminLapiSizeConfig`,
  `interactive.InteractiveLapiSizeConfig`.
- Messages listed in `src/main/size-config/exclusions.txt` are skipped.
- Any other package fails the generation, so that a package added to the Ledger API is not silently
  left without size limits: add it to `generatedPackages` or `ignoredPackages` in the
  generator.

The generated classes are checked in, and compiling uses them as they are. After a change to the
protos, `exclusions.txt` or the generator, regenerate them with
`sbt ledger-api-size-config/protocGenerate` and commit the changes. CI runs
`sbt sizeConfigUpToDateCheck`, which regenerates them and fails if they differ from the checked-in ones.

## `src/main/scala` is owned by the generator

The `src/main/scala` folder contains only generated files. Any hand-written file placed there is
deleted without warning the next time the classes are regenerated.

**Do not edit the generated files and do not add files to `src/main/scala`.**
