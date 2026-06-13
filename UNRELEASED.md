# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Minor Improvements
- improvement

#### Dependencies of vetted packages can be unvetted (PV35+)
On synchronizers running protocol versions 35 and above, vetting state change operations on the API (e.g. via ``/package-vetting/update``)
allow dependencies of vetted packages to be unvetted safely, without requiring the use of a force flag.
This is consistent with the transaction protocol, which does not require the dependencies of a Daml transaction node package to be vetted in PV35+.
**Note**: For protocol versions 34 and below, the package dependency vetting restrictions remain unchanged.

### Preview Features
- preview feature

## Bugfixes

### (26-003, High): Restarting stuck sequencer yields to sequencer fork

#### Issue Description
Around LSU: if a sequencer not processing blocks is restarted after upgrade time and before it had the chance to process any block,
it will delete traffic data entries as part of crash recovery.

#### Affected Deployments
Sequencer nodes

#### Affected Versions
All versions before 3.5.5

#### Impact
Sequencer fork

#### Symptom
Inability for participants to connect (cannot get consensus on traffic) or participant disconnecting with SEQUENCER_FORK_DETECTED

#### Workaround
Restore from backup

#### Likeliness
Exceptional

#### Recommendation
Upgrade to 3.5.5

## Compatibility

The following Canton protocol versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | PROTOCOL_VERSIONS          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | JAVA_VERSION               |
| Postgres                   | POSTGRES_VERSION           |


## What's Coming

We are currently working on

