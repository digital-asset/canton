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

### Preview Features
- preview feature

## Bugfixes

### CantonBFT: lower log level of dissemination logs that don't require special attention from WARN to INFO

Some batch dissemination logs in CantonBFT were logged at WARN level, which is too high because they don't
imply abnormal conditions nor non-compliant behavior. This change lowers their log level to INFO.

### (26-005, Major): LSU: Cancelled LSU leftover state prevents PN startup after a successful LSU

#### Issue Description
Cancelled LSU left stale state that combined with a subsequent successful LSU state would prevent a participant node from starting up due to it erroneously recognizing the state mixture as an incomplete LSU and failing to complete it.

#### Affected Deployments
Participant nodes

#### Affected Versions
All versions before 3.5.11

#### Impact
Participants will not start up if restarted for any reason

#### Symptom
Participant node does not start with a log message: `Attempting to run Startup participant node failed with Unable to finish upgrade ...`

#### Workaround
Do not restart a PN before upgrading to a non-affected version. For the cancelled DevNet LSU to the physical synchronizer id `::35-4` the issue could be mitigated by running a database query:
```
update par_synchronizer_connection_configs
set
  status = 'I'
where
  physical_synchronizer_id = 'global-domain::1220be58c29e65de40bf273be1dc2b266d43a9a002ea5b18955aeef7aac881bb471a::35-4';
```

#### Likeliness
The issue deterministically happens on a restart of the participant node, regardless of the restart cause, including normal operational procedures.

#### Recommendation
Upgrade to 3.5.11

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

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

