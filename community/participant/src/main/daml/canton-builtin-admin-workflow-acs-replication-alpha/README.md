Updating canton-builtin-admin-workflow-acs-replication-alpha.dar
==================================================================

The canton-builtin-admin-workflow-acs-replication-alpha.dar implements a
propose-accept pattern to reach agreement among participants on how to perform
ACS replication.

Normal builds use the checked-in DAR in `src/main/resources/dar/` and do not rebuild this Daml
project, including when running `damlBuild`. After changing the Daml model, explicitly rebuild the
checked-in DAR from the repository root:

```sh
sbt --batch 'community-participant / Compile / damlPinProjects'
```

The changes must be backward compatible according to the smart contract upgrading (SCU)
constraints, e.g. check this by uploading the rebuilt dar to a participant started with the prior
version:

```
$ ./bin/canton -c examples/01-simple-topology/simple-topology.conf --bootstrap examples/01-simple-topology/simple-ping.canton
@ participant1.dars.upload("community/participant/src/main/resources/dar/canton-builtin-admin-workflow-acs-replication-alpha.dar")
```

and ensuring that there is no error such as
`INVALID_ARGUMENT/NOT_VALID_UPGRADE_PACKAGE(8,..): Upgrade checks indicate that .. cannot be an upgrade of ...`.
