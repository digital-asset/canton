Updating canton-traffic-credit.dar
==================================

This package is depended on by applications that sell traffic (e.g. wallet providers), and it is
read by the traffic enforcement (TEA) top-up ingestion. Participants with Daml top-up enabled upload
and vet the DAR that the generated bindings come from (`TeaCreditIngestionService.DarResourceFileName`).

Normal builds and Java codegen use the checked-in DAR. Before updating it, preserve the previous
DAR, bump the package version in `daml.yaml`, and ensure the model changes are backward compatible
according to the smart contract upgrading (SCU) constraints.

Explicitly rebuild the pinned DAR from the repository root (`damlBuild` skips pinned projects):

```sh
sbt --batch 'traffic-enforcement-component / Compile / damlPinProjects'
```

Ensure that there is no error such as
`INVALID_ARGUMENT/NOT_VALID_UPGRADE_PACKAGE(8,..): Upgrade checks indicate that .. cannot be an upgrade of ...`.

SCU compatibility alone does not guarantee ingestion compatibility. TEA currently accepts only the
package ID from its generated bindings. A new DAR would have a new package ID, so TEA must add it
to both the subscription filter and event decoding. We need to keep accepting the previous package
IDs until no deployed wallet uses them and they are no longer needed for historical replay.

