Updating canton-traffic-credit.dar
==================================

This package is depended on by applications that sell traffic (e.g. wallet providers), and it is
read by the traffic enforcement (TEA) top-up ingestion. Participants with Daml top-up enabled upload
and vet the DAR that the generated bindings come from (`TeaCreditIngestionService.DarResourceFileName`).

Normal builds and Java codegen use the checked-in DAR. The package defines the
`TrafficCreditIssuer` interface, which applications implement, and must not contain templates.
Interfaces cannot be upgraded under smart contract upgrading (SCU): any change to the interface
fails the upgrade checks, so a change requires a new interface in a new package with a different
name.

Explicitly rebuild the pinned DAR from the repository root (`damlBuild` skips pinned projects):

```sh
sbt --batch 'traffic-enforcement-component / Compile / damlPinProjects'
```
