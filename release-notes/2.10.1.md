# Release of Canton 2.10.1

Canton 2.10.1 has been released on May 30, 2025. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/v2.10.1). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-2.10.1.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/2.10.1/canton/about.html).

## Summary

This is a maintenance release that fixes one high, one medium, and two low severity issues.
Please update during the next maintenance window.

## What’s New

### Daml packages validation on Ledger API start-up
On Ledger-API start-up, the Daml package store of the participant node
is checked for upgrade compatibility for all persisted packages.
On compatibility check failure, the participant is shut down with an error message.
To disable the check (not recommended), set `canton.participants.participant.parameters.unsafe-disable-upgrade-validation=true`.

### JWT Tokens in Admin API
#### Background
User authorization is extended to all service types for participant nodes in 2.10 for the Canton Admin API.

#### Specific Changes
The users is able to configure authorization on the Admin API of the participant node in a manner similar to what
is currently possible on the Ledger API. However, it is necessary to specify explicitly which users are
allowed in and which grpc services are accessible to them. An example configuration for both Ledger and Admin API
looks like this:

```
canton {
  participants {
    participant {
      ledger-api {
        port = 5001
        auth-services = [{
          type = jwt-rs-256-jwks
          url = "https://target.audience.url/jwks.json"
          target-audience = "https://rewrite.target.audience.url"
        }]
      }
      admin-api {
        port = 5002
        auth-services = [{
          type = jwt-rs-256-jwks
          url = "https://target.audience.url/jwks.json"
          target-audience = "https://rewrite.target.audience.url"
          users = [{
            user-id = alice
            allowed-services = [{
              "admin.v0.ParticipantRepairService",
              "connection.v30.ApiInfoService",
              "v1alpha.ServerReflection",
            }]
          }]
        }]
      }
    }
  }
}
```

While the users appearing in the sub claims of the JWT tokens on the Ledger API always have to be present in
the participant’s user database, no such requirement exists for the Admin API. The user in the authorization service
config can be an arbitrary choice of the participant’s operator. This user also needs to be configured in the associated
IDP system issuing the JWT tokens.

The configuration can contain a definition of either the target audience or the target scope depending on the specific
preference of the client organization. If none is given, the JWT tokens minted by the IDP must specify daml_ledger_api
as their scope claim.

Independent of the specific service that the operator wants to expose, it is a good practice to also give access rights
to the ServerReflection service. Some tools such as grpcurl or postman need to hit that service to construct their requests.

#### Impact and Migration
The changes are backwards compatible.

### LF 1.17 templates cannot implement LF 1.15 interfaces.

### Background

Bug 25-005, detailed below, prevents the execution of choices from LF
1.15 interfaces within LF 1.17 templates. To resolve this, we have
entirely restricted LF 1.17 templates from implementing LF 1.15
interfaces. However, this change is more than just a bug fix—it
reflects a deliberate alignment with upgradeability principles.
While mixing LF 1.15 interfaces with LF 1.17 templates may seem
advantageous, it is only useful if your model contains other LF 1.15
templates implementing those interfaces. Yet, this approach ultimately
disrupts the upgrade path. Maintaining two versions of a template
within the same model leads to inconsistency: LF 1.17 templates
support seamless upgrades, whereas LF 1.15 templates do not, requiring
an offline migration to fully upgrade the model.


### Specific Changes

- The compiler now prevents Daml models from implementing an LF 1.15
  interface within an LF 1.17 template.

- For backward compatibility, participants can still load these models
  if they were compiled with SDK 2.10.0. However:
  - A warning is issued at load time, if those models contain an LF 1.17 template implementing an LF 1.15 interface.
  - Any attempt to execute a choice on an LF 1.15 interface within an
    LF 1.17 template (compiled with SDK 2.10.0) will trigger a runtime
    error during submission.

### Impact and Migration

These changes preserve backward compatibility, while preventing
the participant from crashing.

### Minor Improvements
- The daml values representing parties received over the Ledger API can be validated in a stricter manner. When the
  `canton.participants.<participant>.http-ledger-api.daml-definitions-service-enabled` parameter is turned on,
  the parties must adhere to a format containing the hint and the fingerprint separated by a double colon:
  `<party-hint>::<fingerprint>`. The change affects the values embedded in the commands supplied to the `Submit*` calls
  to the `CommandSubmissionService` and the `CommandService`.
- Added configuration for the size of the inbound metadata on the Ledger API. Changing this value allows
  the server to accept larger JWT tokens.
  `canton.participants.participant.ledger-api.max-inbound-metadata-size=10240`
- `canton.participants.participant.parameters.disable-upgrade-validation` is explicitly deemed a dangerous configuration by:
  - renaming it to `unsafe-disable-upgrade-validation`.
  - only allowing its setting in conjunction with `canton.parameters.non-standard-config = yes`

## Bugfixes

### (25-005, Low): Cannot exercise LF 1.15 interface choices on LF 1.17 contracts

#### Issue Description

Daml-LF 1.17 versions exercise-by-interface nodes according to the
interface's language version.  This ensures that the choice argument
and result are not normalized if the interface is defined in LF 1.15,
which is important for Ledger API clients that have been compiled against
the interface and expect non-normalized values. Yet, the package name
is still set on the exercise node based on the contract's language
version, namely 1.17.

This leads to at least two problems:

- When Canton tries to serialize the transaction in Phase 7, the
  `TransactionCoder` attempts to serialize the node with version 1.15
  into the SingleDimensionEventLog, but cannot do so because package
  names cannot be serialized in 1.15. This serialization exception
  bubbles up into the application handler and causes the participant
  to disconnect from the domain. This problem is sticky in that crash
  recovery will run into the same problem again and disconnect
  again.

- When the template defines a key, Canton serializes the global key
  for the view participant according to the exercise node
  version. Accordingly, the hash of the key according to LF
  1.15. This trips up the consistency check in ViewParticipantData
  because the input contract's serialized key was hashed according to
  LF 1.17. This failure happens only Phase 3 during when decrypting
  and parsing the received views. Participants discard such views and
  reject the confirmation request. The failure does not happen in
  Phase 1 because the global key hashes are still correct in Phase 1.
  Serialization correctly sets the package name, but uses the wrong
  version (1.15) from the exercise node. Deserialization sees the
  wrong version and ignores the package name.
  Accordingly, the participant discards the views it cannot
  deserialize and rejects the confirmation request if it is a
  confirming node.

#### Affected Deployments
Participant nodes.

#### Affected Versions

- 2.10.0

#### Impact

- Participant node crashes and may need manual repair.
- Command submission fails with `INVALID_ARGUMENT`.

#### Symptom

If the template defines a contract key, the participant logs
`LOCAL_VERDICT_MALFORMED_PAYLOAD` with error message "Inconsistencies
for resolved keys: GlobalKey(...) -> Assigned(...)" and rejects the
confirmation request. The command submission fails with
`INVALID_ARGUMENT`

If the template does not define a contract key, the transaction gets
processed, but the participant fails to store the accepted transaction
in its database with the error message "Failed to serialize versioned
transaction: packageName is not supported by transaction version 15",
disconnects from the domain with an `ApplicationHandlerFailure` and
possible crashes (depending on configuration). Reconnection attempts
will run into the same problem over and over gain.

#### Workaround

Recompile the interfaces with LF 1.17. This may break ledger API
clients that expected trailing None record fields in exercise arguments
and results.

#### Likeliness

Deterministic.

#### Recommendation

Upgrade to 2.10.1. If your Daml models contain LF 1.17 templates
implementing LF 1.15 interfaces, and you face issues rewriting
everything in LF 1.17, please contact Digital Asset support.

### (25-006, High): Confidential configuration fields are logged in plain text when using specific configuration syntax or CLI flags

#### Issue Description
If the `logConfigWithDefaults` config parameter is set to false (which is the default),
the config rendering logic fails to redact confidential information (e.g DB credentials) when config substitution is used in combination with a config element override.

Suppose we have the following configuration file:

_canton.conf_
```hocon
_storage {
  password = confidential
}
canton {
  storage = ${_storage}
}
```

Now the confidential config element is changed via another config file:

_override.conf_
```hocon
canton.storage.password = confidential2
```
and then:
```shell
canton -c canton.conf -c override.conf
```

This exposes the confidential `password` field in the log file.

Alternatively the config element can be changed as well via the CLI:

```shell
canton -c canton.conf -C "canton.storage.password=confidential2"
```

In both cases the password field will not be redacted as it should when the config is logged.

#### Affected Deployments
All nodes.

#### Affected Versions

- 2.10.0
- 2.9.0-2.9.7
- 2.8.0-2.8.12

#### Impact
Confidential information in the config will be logged during node startup if one of the conditions described is met.
Confidential information like credentials may be used to gain unauthorized access by an attacker who has access to the canton logs and network-level access to the affected systems like a database server with an exposed database credential.

#### Symptom
During startup, if the node is configured to log its configuration without defaults values (default behavior), confidential information that should otherwise be redacted may be logged.

#### Workaround
Update the config to not use the structure described in the issue for any confidential config field.

#### Likeliness
Deterministic if the configuration uses the structure described in the issue.

#### Recommendation
Upgrade in the next maintenance window if affected by this issue. Roll the exposed credentials.

### (25-004, Medium): RepairService contract import discards re-computed contract keys in the repaired contract

#### Issue Description
The repair service re-computes contract metadata when adding new contracts.
However, instead of repairing the contract with the re-computed keys, it re-uses the keys from the input contract.
Combined with a gap in the console macros which do not propagate contract keys during ACS export,
migrating contracts with keys in that way can result in an inconsistency between the ACS and contract key store,
which crashes the participant when attempting to fetch a contract by key.

#### Affected Deployments
Participant nodes.

#### Affected Versions

- 2.10.0
- 2.9.0-2.9.6
- 2.8.x

#### Impact
Contracts with keys cannot be used after migration via ACS export / import.

#### Symptom
The participant crashes with
"java.lang.IllegalStateException: Unknown keys are to be reassigned. Either the persisted ledger state corrupted or this is a malformed transaction"
when attempting to lookup a contract by key that has been migrated via ACS export / import

#### Workaround
No workaround available. Update to 2.10.1 if affected.

#### Likeliness
Deterministic if migrating contracts with keys using ACS export to an unpatched version.

#### Recommendation
Upgrade to 2.10.1 if affected by this issue.

### (25-007, Low) Insufficient package compatibility upload check for 1.15 packages

#### Issue Description

When a DAR/package is uploaded to a participant node in 2.x, Canton
checks the upgrade compatibility against the DARs already present in
participant's DAR/package stores. Together with the invariant that all
active contracts must have their creation package in the store; this
ensures that the stakeholder participants of a contract will only vet
packages that are compatible with the creation package of the
contract.

These upload checks are insufficient in 2.10.0: For an LF 1.17 package
with a data dependency on a LF 1.15 package, the compatibility check
at upload compares neither the package IDs nor the contents of the
referenced LF 1.15 packages, only the package names, the package
versions, and the module names and identifiers matter.

Suppose that LF 1.17 package `pgkId2'` upgrades the LF 1.17 package
`pkgId2`, i.e., `pkgId2'` has the same package name as `pkgId2` and a
higher version number. The only relevant difference between `pgkId2`
and `pgkId2'` is that `pgkId2` declares a data dependency on the LF
1.15 package `pkgId1` with package name `pkgName1` and version `x`
whereas `pgkId2'` declares the data dependency on the LF 1.15 package
`pkgId1'` with package name `pgkName1` and version `y` with `y` >
`x`. Then, the compatibility check for `pgkId2` and `pgkId2'` will
pass independently of the contents of `pkgId1` and `pkgId1'`, because
LF 1.15 packages are not upgradable.

Moreover, the Daml engine assumes that all contracts read from the index
DB ia type correct, i.e., it does not reverify them. This leads to
the following problems when using a contract created with pkgId2 with
the target package `pkgId2'`:

- On the one hand, if the type or value serialization differs, the
  engine's behavior is entirely unspecified—it may fail with an
  internal error or silently produce an undefined output. In such
  cases, even determinism is not formally guaranteed, yet a code
  review has not identified any instances of nondeterministic
  behavior.

- On the other hand, if the types differ, but the value serialization
  is the same, command interpretation will succeed and Canton commits
  the transaction. However, the result may not be what was intended.

A concrete example:

```
-- in pkgId1
data IouData = IouData with
  owner: Party
  issuer: Party

-- in pgkId1', the order of the fields is swapped
data IouData = IouData with
  issuer: Party
  owner: Party

-- in pgkId2 and pgkId2':
template Iou with
    data: IouData
    amount: Double
  where
    signatory data.owner, data.issuer

    choice Transfer: ContractId Iou with
        newOwner: Party
      controller data.owner, newOwner
      do
      create Iou with
        data = IouData { owner = newOwner, issuer = data.issuer }
        amount = amount
```

Since the order of the IouData fields are flipped in `pgkId1'`, this
means that the roles of owner and issuer are flipped when the contract
choice executes with `pkgId2'`. That is, the issuer can now transfer the
owner's IOU and the owner cannot anymore.

#### Affected Deployments

Participant nodes.

#### Affected Versions

- 2.10.0

#### Impact

- Command submission fails with `INVALID_ARGUMENT`.

- Unexpected behavior of the Daml model.

- Ledger fork is not entirely ruled out.

#### Workaround

Recompile all packages with LF 1.17. However, this may break backward
compatibility for clients expecting a trailing None field in records.

#### Likeliness

Should be deterministic.

#### Recommendation

Upgrade to 2.10.1. Before upgrading, verify that the package store of
each of your participants does not contain invalid upgrades by running
the following script in a Canton console connected to the respective
participant. If you encounter any errors, contact Digital Asset
support.


```remote_participant.ledger_api.packages.check_upgrade_validity```

where `remote_participant` is a reference to a remote Canton participant on version `2.10.0`.

### (25-008, None) Race between multi-build and multi-IDE

#### Issue Description

Multiple IDE instances may start processes that create a package
database while a multi-build is running. These simultaneous operations
can lead to collisions, corrupting the package database.

Running a `daml build ` while interacting with `daml studio` can sometimes collide when creating/updating the package-database, leading to corruption.

#### Affected Deployments

None.

#### Affected Versions

- 2.10.0
- 2.9.x

#### Impact

The Daml build fails, resulting in a suboptimal user experience.

#### Symptom

`daml build` fails with a random files missing. Here is an example of error message:
```
Could not find module `Daml.Script'
It is not a module in the current program, or in any known package.
```

Also, for a package that has interfaces it throws an error:
```
2025-04-22 12:41:12.32 [INFO]  [multi-package build]
Building C:/tmp/v2/
damlc: /tmp/v1/.daml\dependencies\1.17\e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b: getDirectoryContents:findFirstFile: does not exist (The system cannot find the path specified.)
File:     /tmp/v1/
Hidden:   no
Range:    1:1-2:1
Source:   compiler
Severity: DsError
Message:
Failed to build package at /tmp/v1/.
CallStack (from HasCallStack):
  error, called at compiler\damlc\lib\DA\Cli\Damlc.hs:1240:44 in compilerZSdamlcZSdamlc-lib:DA.Cli.Damlc
```

#### Workaround

Close the IDE, kill all `damlc` processes, and then run `daml clean --all` followed by `daml build --all`.

#### Likeliness

Undeterministic.

#### Recommendation

Update your SDK to 2.10.1 or later.

### (25-009, None) Upgrade runtime error details are swallowed by the IDE

#### Issue Description

The IDE does not show the error details when a runtime error occurs
during the upgrade process. This is because the IDE does not propagate
the error details from the Canton server to the client.

#### Affected Deployments

None.

#### Affected Versions

- 2.10.0
- 2.9.x

#### Impact

Daml script fails without details, resulting in a suboptimal user experience.

#### Symptom

Runtime upgrade errors are reported as `<missing error details>` in the IDE.

#### Workaround

Run the script against the sandbox.

#### Likeliness

Deterministic.

#### Recommendation

Update your SDK to 2.10.1 or later.

### (25-010, None) Compiler emits wrong upgrade warning about changed precondition

#### Issue Description

The compiler performs best-effort checks to verify that the
expressions defining the ensure clause, signatories, observers, or the
key of a template remain unchanged between templates in an upgrade
relationship. If a change is detected, a warning is emitted. However,
if the precondition expression calls a utility package—such as the
standard library—a warning may be erroneously triggered.

#### Affected Deployments

None.

#### Affected Versions

- 2.10.0

#### Impact

Compilation emits an erroneous warning, resulting in a suboptimal user experience.

#### Symptom

The compiler emit a warning of the form:

```
warning while type checking template Main.U precondition:
The upgraded template U has changed the definition of its precondition.
There is 1 difference in the expression:
Name fde31f3683d30c9638fbd0ef4420e1acd5938673e96dc68db28ac85d6ad81b50:Dep:someBool and name f70bec8f7a75adf6649f9a5dcd3b95b4ff2daefafc88e81670fdc1d48ede85cc:Dep:someBool differ for the following reason: Just Name came from package 'dep (fde31f3683d30c9638fbd0ef4420e1acd5938673e96dc68db28ac85d6ad81b50) (v0.1)' and now comes from package 'dep (f70bec8f7a75adf6649f9a5dcd3b95b4ff2daefafc88e81670fdc1d48ede85cc) (v0.2)'. Both packages support upgrades, but the previous package had a higher version than the current one.
Upgrade this warning to an error -Werror=upgraded-template-expression-changed
Disable this warning entirely with -Wno-upgraded-template-expression-changed
```

This warning is incorrect because it incorrectly states that version
v0.1 has a higher version than v0.2, which is not true. In reality,
the upgrade process is valid, and no warning should be
emitted. Additionally, had the dependency versions been swapped, the
compiler would not have emitted a warning, which is also incorrect
behavior.

#### Workaround

Ignore the warning.

#### Likeliness

Deterministic.

#### Recommendation

Update you SDK to 2.10.1 or later.

### (25-011, None) IDE does not close daml processes when closed

#### Issue Description

An incompatibility between the latest version of VS Code and the VS Code client library we are using causes the Daml process to remain open improperly on Windows and Mac, preventing it from closing as expected.

#### Affected Deployments

None.

#### Affected Versions

- 2.10.0
- 2.9.x

#### Impact

Accumulation of unneeded processes.

#### Workaround

Kill the processes manually.

#### Likeliness

Deterministic.

#### Recommendation

Update you SDK to 2.10.1 or later.

## Compatibility

The following Canton protocol versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | 5, 7          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | OpenJDK 64-Bit Server VM Zulu11.72+19-CA (build 11.0.23+9-LTS, mixed mode)               |
| Postgres                   | Recommended: PostgreSQL 12.22 (Debian 12.22-1.pgdg120+1) – Also tested: PostgreSQL 11.16 (Debian 11.16-1.pgdg90+1), PostgreSQL 11.16 (Debian 11.16-1.pgdg90+1), PostgreSQL 13.21 (Debian 13.21-1.pgdg120+1), PostgreSQL 13.21 (Debian 13.21-1.pgdg120+1), PostgreSQL 14.18 (Debian 14.18-1.pgdg120+1), PostgreSQL 14.18 (Debian 14.18-1.pgdg120+1), PostgreSQL 15.13 (Debian 15.13-1.pgdg120+1), PostgreSQL 15.13 (Debian 15.13-1.pgdg120+1)           |
| Oracle                     | 19.20.0             |

